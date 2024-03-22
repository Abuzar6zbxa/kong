local cjson = require "cjson.safe"
local ngx_ssl = require "ngx.ssl"
local clone = require "table.clone"

local ngx_var = ngx.var
local cjson_encode = cjson.encode
local ipairs = ipairs
local coroutine_running = coroutine.running
local get_ctx_table = require("resty.core.ctx").get_ctx_table
local native_timer_at = _G.native_timer_at or ngx.timer.at


local req_start_time
local req_get_headers
local resp_get_headers


--- keep request data a bit longer, into the log timer
local save_for_later = {}


if ngx.config.subsystem == "http" then
  req_start_time   = ngx.req.start_time
  req_get_headers  = ngx.req.get_headers
  resp_get_headers = ngx.resp.get_headers

else
  local NOOP = function() end

  req_start_time   = NOOP
  req_get_headers  = NOOP
  resp_get_headers = NOOP
end


local _M = {}


local function get_saved()
  return save_for_later[coroutine_running()]
end


_M.exposed_pdk = {
  kong = kong,

  get_saved_for_later = get_saved,

  ["kong.log.serialize"] = function()
    local saved = get_saved()
    return cjson_encode(saved and saved.serialize_data or kong.log.serialize())
  end,

  ["kong.nginx.get_var"] = function(v)
    return ngx_var[v]
  end,

  ["kong.nginx.get_tls1_version_str"] = ngx_ssl.get_tls1_version_str,

  ["kong.nginx.get_ctx"] = function(k)
    local saved = get_saved()
    local ngx_ctx = saved and saved.ngx_ctx or ngx.ctx
    return ngx_ctx[k]
  end,

  ["kong.nginx.set_ctx"] = function(k, v)
    local saved = get_saved()
    local ngx_ctx = saved and saved.ngx_ctx or ngx.ctx
    ngx_ctx[k] = v
  end,

  ["kong.ctx.shared.get"] = function(k)
    local saved = get_saved()
    local ctx_shared = saved and saved.ctx_shared or kong.ctx.shared
    return ctx_shared[k]
  end,

  ["kong.ctx.shared.set"] = function(k, v)
    local saved = get_saved()
    local ctx_shared = saved and saved.ctx_shared or kong.ctx.shared
    ctx_shared[k] = v
  end,

  ["kong.request.get_headers"] = function(max)
    local saved = get_saved()
    return saved and saved.request_headers or kong.request.get_headers(max)
  end,

  ["kong.request.get_header"] = function(name)
    local saved = get_saved()
    if not saved then
      return kong.request.get_header(name)
    end

    local header_value = saved.request_headers[name]
    if type(header_value) == "table" then
      header_value = header_value[1]
    end

    return header_value
  end,

  ["kong.request.get_uri_captures"] = function()
    local saved = get_saved()
    local ngx_ctx = saved and saved.ngx_ctx or ngx.ctx
    return kong.request.get_uri_captures(ngx_ctx)
  end,

  ["kong.response.get_status"] = function()
    local saved = get_saved()
    return saved and saved.response_status or kong.response.get_status()
  end,

  ["kong.response.get_headers"] = function(max)
    local saved = get_saved()
    return saved and saved.response_headers or kong.response.get_headers(max)
  end,

  ["kong.response.get_header"] = function(name)
    local saved = get_saved()
    if not saved then
      return kong.response.get_header(name)
    end

    local header_value = saved.response_headers and saved.response_headers[name]
    if type(header_value) == "table" then
      header_value = header_value[1]
    end

    return header_value
  end,

  ["kong.response.get_source"] = function()
    local saved = get_saved()
    return kong.response.get_source(saved and saved.ngx_ctx or nil)
  end,

  ["kong.nginx.req_start_time"] = function()
    local saved = get_saved()
    return saved and saved.req_start_time or req_start_time()
  end,
}


--- Phase closures
function _M.build_phases(plugin)
  if not plugin then
    return
  end

  for _, phase in ipairs(plugin.phases) do
    if phase == "log" then
      plugin[phase] = function(self, conf)
        native_timer_at(0, function(premature, saved)
          if premature then
            return
          end
          get_ctx_table(saved.ngx_ctx)
          local co = coroutine_running()
          save_for_later[co] = saved
          plugin.rpc:handle_event(self.name, conf, phase)
          save_for_later[co] = nil
        end, {
          plugin_name = self.name,
          serialize_data = kong.log.serialize(),
          ngx_ctx = clone(ngx.ctx),
          ctx_shared = kong.ctx.shared,
          request_headers = req_get_headers(),
          response_headers = resp_get_headers(),
          response_status = ngx.status,
          req_start_time = req_start_time(),
        })
      end

    else
      plugin[phase] = function(self, conf)
        plugin.rpc:handle_event(self.name, conf, phase)
      end
    end
  end

  return plugin
end


return _M
