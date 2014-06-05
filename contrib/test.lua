local fmt, tc = string.format, table.concat

local asr = {}
function asr.equal(a, b) return a == b end
function asr.error(fn, ...) local ok, out = pcall(fn, ...) return (not ok) and out end
function asr.match(a, b) return type(a) == "string" and a:match(b) end
function asr.type(a, b) return type(a) == b end

local a_mt = {}
function a_mt.__index(t, k)
    if not asr[k] then return end
    return function(...)
        local ok = asr[k](...)
        if ok then return ok end
        local args = {}
        for i = 1, select('#', ...) do
            args[#args+1] = tostring(select(i, ...))
        end
        error(fmt("assert.%s failed; args: %s", k, tc(args, ", ")), 2)
    end
end
a_mt.__call = assert
assert = setmetatable({}, a_mt) 

local function fmt_table(header, t)
    local out = {}
    for k, v in pairs(t) do
        out[#out+1] = fmt("%s: %s (%s)", tostring(k), v, type(v))
    end
    return fmt("%s: %s", header, tc(out, ", "))
end

local function fmt_list(header, ...)
    local out = {}
    for i = 1, select('#', ...) do
        local v = select(i, ...)
        out[#out+1](fmt("%s (%s)", tostring(v), type(v)))
    end
    return fmt("%s: %s", header, tc(out, ","))
end

local function writeout(t, num, pass, fail, maxn)
    local errors, fstr = {}, "%02d: %-"..maxn.."s %s"
    local n = 0
    for i, res in ipairs(t) do
        if not res then goto skip end
        n = n + 1
        print(fmt(fstr, n, res.name, res.trace and "FAILED" or "OK"))
        if not res.trace then goto skip end
        local errmsg
        if res.trace:match("\n") then
            errmsg, res.trace = res.trace:match("(.-)\n(.+)")
        end
        errors[#errors+1] = fmt("%s: %s", res.name, errmsg or res.trace)
        if t.show_locals and next(res.locals) ~= nil then
            errors[#errors+1] = fmt_table("locals", res.locals)
        end
        if t.show_upvals and next(res.upvals) ~= nil then
            errors[#errors+1] = fmt_table("upvals", res.upvals)
        end
        if t.show_trace and res.trace then errors[#errors+1] = res.trace end
        errors[#errors+1] = "---"
        ::skip::
    end
    print(fmt("\n%s passed, %s failed, %s skipped\n", pass, fail, num - pass - fail))
    if #errors > 0 then print(tc(errors, "\n")) end
end

local function traceback(v)
    return function(msg)
        local level = 1
        while true do
            local info = debug.getinfo(level) 
            if not info or info.func == v.test then break end
            level = level + 1
        end
        local tb, lc, uv = debug.traceback(msg, level), {}, {}
        for i = 1, math.huge do
            local j, w = debug.getlocal(level, i) 
            if not j then break end
            lc[j] = w
        end
        for i = 1, math.huge do
            local j, w = debug.getupvalue(v.test, i)
            if not j then break end
            uv[j] = w
        end
        v.trace, v.locals, v.upvals = tb, lc, uv
    end
end

local function call(t, ...)
    local panic, num, pass, fail, maxn = false, #t, 0, 0, 8
    for i, v in ipairs(t) do
        local trace = traceback(v)
        if v.name == "ensure" and i <= num then 
            if not panic then table.insert(t, num+1, v) end
            rawset(t, i, false)
            goto skip
        end
        if panic and i <= num then
            t[i] = false
            goto skip
        end
        local ok, tb = xpcall(v.test, trace)
        if v.name == "silent" and ok then
            t[i] = false
            goto skip
        end
        maxn = math.max(maxn, (#v.name)+4)
        pass = ok and pass + 1 or pass
        fail = ok and fail or fail + 1 
        panic = (not ok) and t.break_on_error
        ::skip::
    end
    writeout(t, num, pass, fail, maxn)
end

function newidx(t, k, v)
    if type(v) == "boolean" and t[k] ~= nil then
        getmetatable(t).__index[k] = v 
        return
    end
    if not type(v) == "function" then error("Expected a function") end
    rawset(t, #t+1, { name = k, test = v })
end

return setmetatable({}, { 
    __index = { break_on_error = false, show_trace = true, show_locals = false, show_upvals = false },
    __newindex = newidx, __call = call
})
