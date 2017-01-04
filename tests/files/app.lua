box.once('v1', function()
  box.schema.user.create('t1', {password = 't1'})

  local s = box.schema.create_space('tester')
  s:create_index('primary')
end)
