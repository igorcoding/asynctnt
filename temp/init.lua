box.cfg{
  listen = 3303,
  wal_mode = 'none'
}

box.once('access', function()
  box.schema.user.grant('guest', 'read,write,execute', 'universe')

  box.schema.user.create('tt', {password = 'ttp'})
  -- box.schema.user.grant('tt', 'read,write,execute', 'universe')

  box.schema.user.create('tt2', {password = 'ttp2'})
  box.schema.user.grant('tt2', 'read,write,execute', 'universe')

  local s = box.schema.create_space('tester')
  s:create_index('primary')
end)

local fiber = require('fiber')

function long(t)
  fiber.sleep(t)
  return 'ok'
end

function test()
	return {'hello'}
end

require('console').start()
