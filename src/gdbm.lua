local ffi = require "ffi"

ffi.cdef [[ 

typedef enum {	
    NO_ERROR,
    MALLOC_ERROR,
    BLOCK_SIZE_ERROR,
    FILE_OPEN_ERROR,
    FILE_WRITE_ERROR,
    FILE_SEEK_ERROR,
    FILE_READ_ERROR,
    BAD_MAGIC_NUMBER,
    EMPTY_DATABASE,
    CANT_BE_READER,
    CANT_BE_WRITER,
    READER_CANT_RECOVER,
    READER_CANT_DELETE,
    READER_CANT_STORE,
    READER_CANT_REORGANIZE,
    UNKNOWN_UPDATE,
    ITEM_NOT_FOUND,
    REORGANIZE_FAILED,
    CANNOT_REPLACE
} gdbm_error; 

typedef struct GDBM_DUMMY *GDBM_FILE;

typedef struct datum {
	char *dptr;
	int   dsize;
} datum;

GDBM_FILE gdbm_open(const char *NAME, int BLOCK_SIZE, int FLAGS, int MODE, void (*fatal_func)(const char *));
void      gdbm_close(GDBM_FILE dbf);
int       gdbm_store(GDBM_FILE dbf, datum key, datum content, int flag);
datum     gdbm_fetch(GDBM_FILE dbf, datum key);
int       gdbm_delete(GDBM_FILE dbf, datum key);
datum     gdbm_firstkey(GDBM_FILE dbf);
datum     gdbm_nextkey(GDBM_FILE dbf, datum key);
int       gdbm_reorganize(GDBM_FILE dbf);
void      gdbm_sync(GDBM_FILE dbf);
int       gdbm_exists(GDBM_FILE dbf, datum key);
char     *gdbm_strerror(gdbm_error errno);
int       gdbm_setopt(GDBM_FILE dbf, int option, void *value, int size);
int       gdbm_fdesc(GDBM_FILE dbf);

extern char      *gdbm_version;
extern gdbm_error gdbm_errno;

]]

local C = ffi.load "gdbm"

local pushdatum = function(str, len)
    if ffi.istype("datum", str) then 
        return str 
    end
    return ffi.new("datum", ffi.cast("char*", str), len or #str)
end

local popdatum = function(dtm)
    if dtm.dptr == nil then
        return nil
    end
    return ffi.string(dtm.dptr, dtm.dsize)
end

-- GDBM module

local M = {}

-- open mode
M.READER  = 0
M.WRITER  = 1
M.WRCREAT = 2
M.NEWDB   = 3

M.FAST    = 0x010 -- obsolete, default
M.SYNC    = 0x020
M.NOLOCK  = 0x040
M.NOMMAP  = 0x080
M.CLOEXEC = 0x100

-- store flags
M.INSERT  = 0
M.REPLACE = 1

-- options
local setopt_args = {
    "setcachesize", "fastmode", "setsyncmode", "setcentfree",
    "setcoalesceblks", "setmaxmapsize", "setmmap", "getflags",
    "getmmap", "getcachesize", "getsyncmode", "getcentfree",
    "getcoalesceblks", "getmaxmapsize", "getdbname"
}

-- module functions

M.open = function(name, mode, flags, blksize, final)
    mode = mode or M.READER
    local db = C.gdbm_open(name, blksize or 512, mode, flags or 0666, final or function() end)
    if db == nil then return nil, M.strerror() end
    return db
end

M.version = function()
    return ffi.string(C.gdbm_version)
end

M.strerror = function()
    return ffi.string(C.gdbm_strerror(C.gdbm_errno))
end

-- instance functions

local O = {}

O.close = function(db) return C.gdbm_close(db) end
O.sync  = function(db) return C.gdbm_sync(db) end
O.fdesc = function(db) return C.gdbm_fdesc(db) end

-- FIXME allow specifying key/val lengths
function O:store(key, val, flag)
    local ok = C.gdbm_store(self, pushdatum(key), pushdatum(val), flag)
    if ok == -1 then
        return nil, "Database opened in read-only mode"
    elseif ok == 1 then
        return nil, "Attempt to insert duplicate key"
    end
    return true
end

function O:insert(key, val)
    return O:store(key, val, M.INSERT)
end

function O:replace(key, val)
    return O:store(key, val, M.REPLACE)
end

function O:fetch_raw(key)
    return C.gdbm_fetch(self, pushdatum(key))
end

function O:fetch(key)
    return popdatum(self:fetch_raw(key))
end

function O:first_raw()
    return C.gdbm_firstkey(self)
end

function O:first()
    return popdatum(self:first_raw())
end

function O:next_raw(key)
    return C.gdbm_nextkey(self, pushdatum(key))
end

function O:next(key)
    return popdatum(self:next_raw(key))
end

function O:delete(key)
    return C.gdbm_delete(self, pushdatum(key)) == 0
end

function O:exists(key)
    return C.gdbm_exists(self, pushdatum(key)) > 0
end

-- with apologies to lhf

local iter = function(d, k)
    local v
    if k == nil then
        k = d:first()
    else
        k = d:next(k)
    end
    local v = k and d:fetch(k) or nil
    return k, v
end

function O:pairs()
    return iter, self
end

function O:setopt(opt, val)
    -- FIXME use an int* here if possible
    local pval = ffi.cast("void*", val)
    local out = C.gdbm_setopt(self, opt, pval, ffi.sizeof(pval))
    -- FIXME return bools if appropriate
    if out == -1 then return nil, M.strerror() end
    return tonumber(pval)
end

function O:reorganize()
    local out = C.gdbm_reorganize(self)
    if out == 0 then return true end
    return nil, M.strerror()
end

-- alias functions for setopt
for i, v in ipairs(setopt_args) do
    M[string.upper(v)] = i
    O[v] = function(self, value) 
        return self:setopt(i, val)
    end
end

--

ffi.metatype("datum", { __tostring = popdatum })
ffi.metatype("struct GDBM_DUMMY", { __index = O, __pairs = O.pairs })

return M
