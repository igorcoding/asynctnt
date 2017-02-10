box.once('v1', function()
    box.schema.user.create('t1', {password = 't1'})
    box.schema.user.grant('t1', 'read,write,execute', 'universe')

    local s = box.schema.create_space('tester')
    s:create_index('primary')
    s:create_index('txt', {unique = false, parts = {2, 'string'}})
end)


function truncate()
    local keys = {}
    for _, el in box.space.tester:pairs() do
        table.insert(keys, el[1])
    end

    for _, k in ipairs(keys) do
        box.space.tester:delete({k})
    end
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
