local uuid = require 'uuid'

local function check_version(expected, version)
    -- from tarantool/queue compat.lua
    local fun = require 'fun'
    local iter, op  = fun.iter, fun.operator

    local function split(self, sep)
        local sep, fields = sep or ":", {}
        local pattern = string.format("([^%s]+)", sep)
        self:gsub(pattern, function(c) table.insert(fields, c) end)
        return fields
    end

    local function reducer(res, l, r)
        if res ~= nil then
            return res
        end
        if tonumber(l) == tonumber(r) then
            return nil
        end
        return tonumber(l) > tonumber(r)
    end

    local function split_version(version_string)
        local vtable  = split(version_string, '.')
        local vtable2 = split(vtable[3],  '-')
        vtable[3], vtable[4] = vtable2[1], vtable2[2]
        return vtable
    end

    local function check_version_internal(expected, version)
        version = version or _TARANTOOL
        if type(version) == 'string' then
            version = split_version(version)
        end
        local res = iter(version):zip(expected):reduce(reducer, nil)

        if res or res == nil then res = true end
        return res
    end

    return check_version_internal(expected, version)
end


local function bootstrap()
    local b = {
        tarantool_ver = box.info.version,
        has_new_types = false,
        types = {}
    }

    function b:sql_space_name(space_name)
        if self:check_version({3, 0}) then
            return space_name
        else
            return space_name:upper()
        end
    end

    function b:check_version(expected)
        return check_version(expected, self.tarantool_ver)
    end

    if b:check_version({1, 7, 1, 245}) then
        b.has_new_types = true
        b.types.string = 'string'
        b.types.unsigned = 'unsigned'
        b.types.integer = 'integer'
    else
        b.types.string = 'str'
        b.types.unsigned = 'num'
        b.types.integer = 'int'
    end
    b.types.decimal = 'decimal'
    b.types.uuid = 'uuid'
    b.types.datetime = 'datetime'
    b.types.number = 'number'
    b.types.array = 'array'
    b.types.scalar = 'scalar'
    b.types.any = '*'
    return b
end

_G.B = bootstrap()

function change_format()
    box.space.tester:format({
        {type=B.types.unsigned, name='f1'},
        {type=B.types.string, name='f2'},
        {type=B.types.unsigned, name='f3'},
        {type=B.types.unsigned, name='f4'},
        {type=B.types.any, name='f5'},
        {type=B.types.any, name='f6'},
    })
end

box.schema.func.create('change_format', {setuid=true})


box.once('v1', function()
    box.schema.user.create('t1', {password = 't1'})

    if B:check_version({2, 0}) then
        box.schema.user.grant('t1', 'read,write,execute,create,drop,alter', 'universe')
        box.schema.user.grant('guest', 'read,write,execute,create,drop,alter', 'universe')
    else
        box.schema.user.grant('t1', 'read,write,execute', 'universe')
    end

    local s = box.schema.create_space('tester')
    s:format({
        {type=B.types.unsigned, name='f1'},
        {type=B.types.string, name='f2'},
        {type=B.types.unsigned, name='f3'},
        {type=B.types.unsigned, name='f4'},
        {type=B.types.any, name='f5'},
    })
    s:create_index('primary')
    s:create_index('txt', {unique = false, parts = {2, B.types.string}})

    s = box.schema.create_space('no_schema_space')
    s:create_index('primary')
    s:create_index('primary_hash',
                   {type = 'hash', parts = {1, B.types.unsigned}})
end)

if B:check_version({2, 0}) then
    box.once('v2', function()
        box.execute([[
            CREATE TABLE sql_space (
                id INT PRIMARY KEY,
                name TEXT COLLATE "unicode"
            )
        ]])
        box.execute([[
            CREATE TABLE sql_space_autoincrement (
                id INT PRIMARY KEY AUTOINCREMENT,
                name TEXT
            )
        ]])
        box.execute([[
            CREATE TABLE sql_space_autoincrement_multiple (
                id INT PRIMARY KEY AUTOINCREMENT,
                name TEXT
            )
        ]])
    end)

    box.once('v2.1', function()
        if B:check_version({2, 2}) then
            local s = box.schema.create_space('tester_ext_dec')
            s:format({
                {type=B.types.unsigned, name='f1'},
                {type=B.types.decimal, name='f2'},
            })
            s:create_index('primary')
        end

        if B:check_version({2, 4, 1}) then
            s = box.schema.create_space('tester_ext_uuid')
            s:format({
                {type=B.types.unsigned, name='f1'},
                {type=B.types.uuid, name='f2'},
            })
            s:create_index('primary')
        end

        if B:check_version({2, 10, 0}) then
            s = box.schema.create_space('tester_ext_datetime')
            s:format({
                {type=B.types.unsigned, name='id'},
                {type=B.types.datetime, name='dt'},
            })
            s:create_index('primary')
        end
    end)
end


function make_third_index(name)
    local i = box.space.tester:create_index(name, {unique = true, parts = {3, B.types.unsigned}})
    return {i.id}
end


local function _truncate(sp)
    if sp == nil then
        return
    end

    local keys = {}
    for _, el in sp:pairs() do
        table.insert(keys, el[1])
    end

    for _, k in ipairs(keys) do
        sp:delete({k})
    end
end


function truncate()
    _truncate(box.space.tester)
    _truncate(box.space.no_schema_space)

    local sql_spaces = {
        'sql_space',
        'sql_space_autoincrement',
        'sql_space_autoincrement_multiple',
    }
    for _, sql_space in ipairs(sql_spaces) do
        local variants = {
            sql_space,
            sql_space:upper(),
        }

        for _, variant in ipairs(variants) do
            if box.space[variant] ~= nil then
                box.execute('DELETE FROM ' .. variant)
            end
        end
    end

    _truncate(box.space.tester_ext_dec)
    _truncate(box.space.tester_ext_uuid)
    _truncate(box.space.tester_ext_datetime)
end


_G.fiber = require('fiber')


function func_long(t)
    fiber.sleep(t)
    return 'ok'
end


function func_param(p)
    return {p}
end


function func_param_bare(p)
    return p
end


function func_hello_bare()
    return 'hello'
end


function func_hello()
    return {'hello'}
end

function func_load_bin_str()
    local bin_data = uuid.bin()
    return box.space.tester:insert({
        100, bin_data, 12, 15, 'hello'
    })
end

function raise()
    box.error{reason='my reason'}
end

function async_action()
    if box.session.push then
        for i=1,5 do
            box.session.push('hello_' .. tostring(i))
            require'fiber'.sleep(0.01)
        end
    end

    return 'ret'
end
