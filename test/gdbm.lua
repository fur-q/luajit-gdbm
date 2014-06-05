local ffi  = require "ffi"
local test = require "test.test"
local gdbm = require "ffi.gdbm"

local dbfile, keyname, valname, fakename = "/tmp/test.db", "test", "test", "nop"
local db

test.extern = function()
    assert.match(gdbm.version(), "^GDBM version")
end

test.open = function()
    local nope, err = gdbm.open(dbfile, gdbm.READER)
    assert(not nope, err)
    db, err = gdbm.open(dbfile, gdbm.WRCREAT)
    assert(db, err)
end

test.ensure = function()
    if db ~= nil then db:close() end
    os.remove(dbfile)
end

test.store = function()
    assert(db:store(keyname, valname, gdbm.INSERT))
    assert(not db:store(keyname, valname, gdbm.INSERT))
    assert(db:store(keyname, valname, gdbm.REPLACE))
end

test.fetch = function()
    assert.match(db:fetch(keyname), valname)
    assert(not db:fetch(fakename))
end

test.raw = function()
    local datum = db:fetch_raw(keyname)
    assert(ffi.istype("datum", datum))
    assert(db:store(keyname, datum, gdbm.REPLACE))
end

test.exists = function()
    assert(db:exists(keyname))
    assert(not db:exists(fakename))
end

test.delete = function()
    assert(db:delete(keyname))
    assert(not db:delete(keyname))
end

test.first_next = function()
    for i = 1, 5 do
        db:store(keyname .. i, valname .. i, gdbm.INSERT) 
    end
    local val = db:first()
    assert.match(val, valname .. "[1-5]")
    for i = 2, 5 do
        val = db:next(val)
        assert.match(val, valname .. "[1-5]")
    end
end

test.sync = function()
    db:sync()
end

test.fdesc = function()
    assert(db:fdesc() > 0)
end

--

test.break_on_error = true
test()
