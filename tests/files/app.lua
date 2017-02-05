box.once('v1', function()
  box.schema.user.create('t1', {password = 't1'})
  box.schema.user.grant('t1', 'read,write,execute', 'universe')

  local s = box.schema.create_space('tester')
  s:create_index('primary')
end)
