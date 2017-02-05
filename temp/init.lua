box.cfg{
  listen = 3303,
  wal_mode = 'none'
}

box.once('access', function()
  box.schema.user.grant('guest', 'read,write,execute', 'universe')

  box.schema.user.create('tt', {password = 'ttp'})
  -- box.schema.user.grant('tt', 'read,write,execute', 'universe')

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

require('console').start()
