box.cfg {
    listen = '0.0.0.0:3301'
}

box.once('v1', function()
    pcall(box.schema.user.grant, 'guest', 'read,write,execute', 'universe')

    local s = box.schema.create_space('tester')
    s:create_index('primary')
    s:format({
        {name='id', type='unsigned'},
        {name='name', type='string'},
    })
end)

box.once('v2', function()
    pcall(box.schema.user.grant, 'guest', 'read,write,execute', 'universe')

    box.execute([[
        create table users (
            id int primary key,
            name text
        )
    ]])
end)

require 'console'.start()
