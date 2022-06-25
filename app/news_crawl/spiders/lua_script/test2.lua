function main(splash)
    --splash:init_cookies(splash.args.cookies)
    assert(splash:go{
        splash.args.url,
        headers=splash.args.headers,
        http_method=splash.args.http_method,
        body=splash.args.body,
        })
    --次ページのボタンクリック
    local area = splash:select('div.control-nav > a.control-nav-next')
    area:click()
    --assert(splash:wait(0.5))

    print(splash.args.next_page_url)
    --local next_page_url = splash.args.next_page_url

    --次のページボタンのurlへ変わるまで待機
    --local href = splash:select('div.control-nav > a.control-nav-next[href]')
    while not (splash:select('div.control-nav > a.control-nav-next[href="' .. splash.args.next_page_url .. '"]')) do
        splash:wait(0.1)
    end

    -- local entries = splash:history()
    -- local last_response = entries[#entries].response
    return {
        url = splash:url(),
        -- headers = last_response.headers,
        -- http_status = last_response.status,
        -- cookies = splash:get_cookies(),
        html = splash:html(),
    }
end