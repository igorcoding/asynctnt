box.cfg{
  listen = 3305,
  wal_mode = 'none'
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
  box.schema.user.grant('guest', 'read,write,execute', 'universe')

  box.schema.user.create('tt', {password = 'ttp'})
  -- box.schema.user.grant('tt', 'read,write,execute', 'universe')

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

require('console').start()
