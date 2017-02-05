box.once('v1', function()
    box.schema.user.create('t1', {password = 't1'})
    box.schema.user.grant('t1', 'read,write,execute', 'universe')

    local s = box.schema.create_space('tester')
    s:create_index('primary')
end)


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
