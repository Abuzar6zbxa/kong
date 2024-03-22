local protocol_implementations = {
  ["MsgPack:1"] = "kong.runloop.plugin_servers.rpc.mp_rpc",
  ["ProtoBuf:1"] = "kong.runloop.plugin_servers.rpc.pb_rpc",
}


local _M = {}

function _M.new(plugin, callbacks)
  local server_def = plugin.server_def

  -- TODO error handling: verify 3 callbacks
  --

  local rpc_modname = protocol_implementations[server_def.protocol]
  if not rpc_modname then
    return nil, "unknown protocol implementation: " .. (server_def.protocol or "nil")
  end

  kong.log.notice("[pluginserver] loading protocol ", server_def.protocol, " for plugin ", plugin.name)

  local rpc_mod = require (rpc_modname)
  rpc_mod.get_instance_id = callbacks.get_instance_id
  rpc_mod.reset_instance = callbacks.reset_instance
  rpc_mod.exposed_pdk = callbacks.exposed_pdk

  -- XXX 2nd argument refers to "rpc notifications"
  -- which is NYI in the pb-based protocol
  -- so this applies only to mp-based,
  -- consider moving it to the mp module
  local rpc = rpc_mod.new(server_def.socket, callbacks)

  return rpc
end

return _M
