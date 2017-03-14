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


box.once('v1', function()
    box.schema.user.create('t1', {password = 't1'})
    box.schema.user.grant('t1', 'read,write,execute', 'universe')

    local s = box.schema.create_space('tester')
	s:format({
        {type=B.types.string, name='f1'},
		{type=B.types.string, name='f2'},
		{type=B.types.unsigned, name='f3'},
		{type=B.types.unsigned, name='f4'},
		{type=B.types.any, name='f5'},
    })
    s:create_index('primary')
    s:create_index('txt', {unique = false, parts = {2, B.types.string}})
end)


function make_third_index(name)
	local i = box.space.tester:create_index(name, {unique = true, parts = {3, B.types.unsigned}})
	return {i.id}
end


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

function raise()
	box.error{reason='my reason'}
end
