box.cfg{
    listen = 3305,
    wal_mode = 'none',
    readahead = 1 * 1024 * 1024
}

local function bootstrap()
    local b = {
        tarantool_ver = box.info.version,
        has_new_types = false,
        types = {}
    }

    if b.tarantool_ver >= "1.7.1-245" then
        b.has_new_types = true
        b.types.string = 'string'
        b.types.unsigned = 'unsigned'
        b.types.integer = 'integer'
    else
        b.types.string = 'str'
        b.types.unsigned = 'num'
        b.types.integer = 'int'
    end
    b.types.number = 'number'
    b.types.array = 'array'
    b.types.scalar = 'scalar'
    b.types.any = '*'
    return b
end

_G.B = bootstrap()


box.once('access', function()
    pcall(box.schema.user.grant, 'guest', 'read,write,execute', 'universe')

    box.schema.user.create('tt', {password = 'ttp'})

    box.schema.user.create('t1', {password = 't1'})
    box.schema.user.grant('t1', 'read,write,execute', 'universe')

    local s = box.schema.create_space('tester')
    s:create_index('primary')
end)

local fiber = require('fiber')

function long(t)
    fiber.sleep(t)
    return 'ok'
end

function test()
    return 'hello'
end

function func_param(p)
    return {p}
end

function raise()
    box.error{reason='my reason'}
end

function asyncaction()
    for i=1,10 do
        box.session.push('hello_' .. tostring(i))
        fiber.sleep(0.5)
    end

    return 'hi'
end


local function push_messages(sync)
    for i=1,10 do
        print(i)
        box.session.push('hello_' .. tostring(i), sync)
        --box.session.push(box.execute("select * from users limit 1"), sync)
        fiber.sleep(0.5)
        print('end', i, sync)
    end
end

function asyncaction()
    local sync = box.session.sync()
    --fiber.create(function(sync)
    --    fiber.sleep(5)
        push_messages(sync)
    --end, sync)

    return 'hi'
end

function asyncaction()
    for i=1,10 do
        print(i)
        box.session.push('hello_' .. tostring(i))
        fiber.sleep(0.5)
    end

    return 'hi'
end

require('console').start()
