box.cfg {
    listen = '127.0.0.1:3301',
    wal_mode = 'none'
}

box.once('v1', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')

    local s = box.schema.create_space('tester')
    s:create_index('primary')
    s:format({
        {name='id', type='unsigned'},
        {name='text', type='string'},
    })
end)


function f(...)
    return ...
end


local x = 0

function infinite_push_loop()
    local fiber = require('fiber')
    while true do
        fiber.sleep(2)
        x = x + 1
        box.session.push(x)
    end
end
