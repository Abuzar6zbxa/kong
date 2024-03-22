local proc_mgmt = require "kong.runloop.plugin_servers.process"
local bridge = require "kong.runloop.plugin_servers.bridge"

local type = type
local pairs = pairs

local ngx = ngx
local kong = kong
local ngx_sleep = ngx.sleep

local SLEEP_STEP = 0.1
local WAIT_TIME = 10
local MAX_WAIT_STEPS = WAIT_TIME / SLEEP_STEP

local get_instance_id

--- currently running plugin instances
local running_instances = {}

--- handle notifications from pluginservers
local rpc_notifications = {}


--- serverPid notification sent by the pluginserver.  if it changes,
--- all instances tied to this RPC socket should be restarted.
function rpc_notifications:serverPid(n)
  n = tonumber(n)
  if self.pluginserver_pid and n ~= self.pluginserver_pid then
    for key, instance in pairs(running_instances) do
      if instance.rpc == self then
        running_instances[key] = nil
      end
    end
  end

  self.pluginserver_pid = n
end

local function reset_instances_for_plugin(plugin_name)
  for k, instance in pairs(running_instances) do
    if instance.plugin_name == plugin_name then
      running_instances[k] = nil
    end
  end
end

--- reset_instance: removes an instance from the table.
local function reset_instance(plugin_name, conf)
  --
  -- the same plugin (which acts as a plugin server) is shared among
  -- instances of the plugin; for example, the same plugin can be applied
  -- to many routes
  -- `reset_instance` is called when (but not only) the plugin server died;
  -- in such case, all associated instances must be removed, not only the current
  --
  reset_instances_for_plugin(plugin_name)

  local ok, err = kong.worker_events.post("plugin_server", "reset_instances", { plugin_name = plugin_name })
  if not ok then
    kong.log.err("failed to post plugin_server reset_instances event: ", err)
  end
end

local protocol_implementations = {
  ["MsgPack:1"] = "kong.runloop.plugin_servers.mp_rpc",
  ["ProtoBuf:1"] = "kong.runloop.plugin_servers.pb_rpc",
}

local function get_server_rpc(plugin)
  local server_def = plugin.server_def

  if not server_def.rpc then

    local rpc_modname = protocol_implementations[server_def.protocol]
    if not rpc_modname then
      return nil, "unknown protocol implementation: " .. (server_def.protocol or "nil")
    end

    kong.log.notice("[pluginserver] loading protocol ", server_def.protocol, " for plugin ", plugin.name)

    local rpc = require (rpc_modname)
    rpc.get_instance_id = get_instance_id
    rpc.reset_instance = reset_instance
    rpc.exposed_pdk = bridge.exposed_pdk

    server_def.rpc = rpc.new(server_def.socket, rpc_notifications)
                                                -- XXX 2nd argument refers to "rpc notifications"
                                                -- which is NYI in the pb-based protocol
                                                -- so this applies only to mp-based,
                                                -- consider moving it to the mp module
  end

  return server_def.rpc
end

-- module cache of loaded external plugins
-- XXX do we need to invalidate - eg, when the pluginserver restarts?
local loaded_plugins

local function load_external_plugins()
  if loaded_plugins then
    return true
  end

  loaded_plugins = {}

  local kong_config = kong.configuration

  local plugins_info = proc_mgmt.load_external_plugins_info(kong_config)
  assert(next(plugins_info), "failed loading external plugins")

  for plugin_name, plugin in pairs(plugins_info) do
    local plugin = bridge.build_phases(plugin)
    local rpc, err = get_server_rpc(plugin)
    if not rpc then
      return nil, err
    end

    plugin.rpc = rpc
    loaded_plugins[plugin_name] = plugin
  end

  return loaded_plugins
end

local function get_plugin(plugin_name)
  assert(load_external_plugins())

  return loaded_plugins[plugin_name]
end

--- get_instance_id: gets an ID to reference a plugin instance running in the
--- pluginserver; each configuration of a plugin is handled by a different
--- instance.  Biggest complexity here is due to the remote (and thus non-atomic
--- and fallible) operation of starting the instance at the server.
function get_instance_id(plugin_name, conf)
  local key = type(conf) == "table" and kong.plugin.get_id() or plugin_name
  local instance_info = running_instances[key]

  local wait_count = 0
  while instance_info and not instance_info.id do
    -- some other thread is already starting an instance
    -- prevent busy-waiting
    ngx_sleep(SLEEP_STEP)

    -- to prevent a potential dead loop when someone failed to release the ID
    wait_count = wait_count + 1
    if wait_count > MAX_WAIT_STEPS then
      running_instances[key] = nil
      return nil, "Could not claim instance_id for " .. plugin_name .. " (key: " .. key .. ")"
    end
    instance_info = running_instances[key]
  end

  if instance_info
    and instance_info.id
    and instance_info.seq == conf.__seq__
    and instance_info.conf and instance_info.conf.__plugin_id == key
  then
    -- exact match, return it
    return instance_info.id
  end

  local old_instance_id = instance_info and instance_info.id
  if not instance_info then
    -- we're the first, put something to claim
    instance_info          = {
      conf = conf,
      seq = conf.__seq__,
    }
    running_instances[key] = instance_info
  else

    -- there already was something, make it evident that we're changing it
    instance_info.id = nil
  end

  local plugin = get_plugin(plugin_name)

  local new_instance_info, err = plugin.rpc:call_start_instance(plugin_name, conf)
  if new_instance_info == nil then
    kong.log.err("starting instance: ", err)
    -- remove claim, some other thread might succeed
    running_instances[key] = nil
    error(err)
  end

  instance_info.id = new_instance_info.id
  instance_info.plugin_name = plugin_name
  instance_info.conf = new_instance_info.conf
  instance_info.seq = new_instance_info.seq
  instance_info.Config = new_instance_info.Config
  instance_info.rpc = new_instance_info.rpc

  if old_instance_id then
    -- there was a previous instance with same key, close it
    plugin.rpc:call_close_instance(old_instance_id)
    -- don't care if there's an error, maybe other thread closed it first.
  end

  return instance_info.id
end

--- module table
local _M = {}

function _M.load_plugin(plugin_name)
  local plugin = get_plugin(plugin_name)
  if plugin and plugin.PRIORITY then
    return true, plugin
  end

  return false, "no plugin found"
end

function _M.load_schema(plugin_name)
  local plugin = get_plugin(plugin_name)
  if plugin and plugin.PRIORITY then
    return true, plugin.schema
  end

  return false, "no plugin found"
end

function _M.start()
  -- in case plugin server restarts, all workers need to update their defs
  kong.worker_events.register(function (data)
    reset_instances_for_plugin(data.plugin_name)
  end, "plugin_server", "reset_instances")

  assert(proc_mgmt.start_pluginservers())

  return true
end

function _M.stop()
  assert(proc_mgmt.stop_pluginservers())

  return true
end


return _M
