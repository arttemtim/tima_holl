import json
import asyncio
import aiofiles
import requests
from aiocsv import AsyncWriter

# cookies = {
#     '_slfs': '1700109451896',
#     '_slid': '65559c8c32594c9c130c1db8',
#     '_slsession': 'CF0C1A38-D7E1-45F6-96CA-7A0A6ECA27E5',
#     'metro_api_session': 'TFnlBT9Fqit4k17ipQf8NWeLWOFJfYFxcMLllIay',
#     '_slfreq': '633ff97b9a3f3b9e90027740%3A633ffa4c90db8d5cf00d7810%3A1700116654%3B64a81e68255733f276099da5%3A64abaf645c1afe216b0a0d38%3A1700116654',
#     '_ym_uid': '1700109454583445248',
#     '_ym_d': '1700109454',
#     '_ga': 'GA1.1.15995169.1700109455',
#     '_ym_isad': '2',
#     'mp_6d4d5c2aa2170f906c84f873ab89181b_mixpanel': '%7B%22distinct_id%22%3A%20%22%24device%3A18bd66b9256238f-0be5755e3b02a9-1e462c6c-1fa400-18bd66b9256238f%22%2C%22%24device_id%22%3A%20%2218bd66b9256238f-0be5755e3b02a9-1e462c6c-1fa400-18bd66b9256238f%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D',
#     '_ym_visorc': 'w',
#     'uxs_uid': 'de795540-8439-11ee-b261-af7f4d434e65',
#     '_gcl_au': '1.1.908701163.1700109458',
#     'tmr_lvid': 'a49ce1a3e550dd0b9890bc735ea212db',
#     'tmr_lvidTS': '1700109458287',
#     'mindboxDeviceUUID': 'fcdf41c3-6330-4673-8882-e19b90f25520',
#     'directCrm-session': '%7B%22deviceGuid%22%3A%22fcdf41c3-6330-4673-8882-e19b90f25520%22%7D',
#     '_ga_VHKD93V3FV': 'GS1.1.1700109455.1.0.1700109483.0.0.0',
# }

headers = {
    'authority': 'api.metro-cc.ru',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ru-RU,ru;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    # 'cookie': '_slfs=1700109451896; _slid=65559c8c32594c9c130c1db8; _slsession=CF0C1A38-D7E1-45F6-96CA-7A0A6ECA27E5; metro_api_session=TFnlBT9Fqit4k17ipQf8NWeLWOFJfYFxcMLllIay; _slfreq=633ff97b9a3f3b9e90027740%3A633ffa4c90db8d5cf00d7810%3A1700116654%3B64a81e68255733f276099da5%3A64abaf645c1afe216b0a0d38%3A1700116654; _ym_uid=1700109454583445248; _ym_d=1700109454; _ga=GA1.1.15995169.1700109455; _ym_isad=2; mp_6d4d5c2aa2170f906c84f873ab89181b_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A18bd66b9256238f-0be5755e3b02a9-1e462c6c-1fa400-18bd66b9256238f%22%2C%22%24device_id%22%3A%20%2218bd66b9256238f-0be5755e3b02a9-1e462c6c-1fa400-18bd66b9256238f%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D; _ym_visorc=w; uxs_uid=de795540-8439-11ee-b261-af7f4d434e65; _gcl_au=1.1.908701163.1700109458; tmr_lvid=a49ce1a3e550dd0b9890bc735ea212db; tmr_lvidTS=1700109458287; mindboxDeviceUUID=fcdf41c3-6330-4673-8882-e19b90f25520; directCrm-session=%7B%22deviceGuid%22%3A%22fcdf41c3-6330-4673-8882-e19b90f25520%22%7D; _ga_VHKD93V3FV=GS1.1.1700109455.1.0.1700109483.0.0.0',
    'origin': 'https://online.metro-cc.ru',
    'pragma': 'no-cache',
    'referer': 'https://online.metro-cc.ru/',
    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
}

json_data = {
    'query': '\n  query Query($storeId: Int!, $slug: String!, $attributes:[AttributeFilter], $filters: [FieldFilter], $from: Int!, $size: Int!, $sort: InCategorySort, $in_stock: Boolean, $eshop_order: Boolean, $is_action: Boolean, $price_levels: Boolean) {\n    category (storeId: $storeId, slug: $slug, inStock: $in_stock, eshopAvailability: $eshop_order, isPromo: $is_action, priceLevels: $price_levels) {\n      id\n      name\n      slug\n      id\n      parent_id\n      meta {\n        description\n        h1\n        title\n        keywords\n      }\n      disclaimer\n      description {\n        top\n        main\n        bottom\n      }\n#      treeBranch {\n#        id\n#        name\n#        slug\n#        children {\n#          category_type\n#          id\n#          name\n#          slug\n#          children {\n#            category_type\n#            id\n#            name\n#            slug\n#            children {\n#              category_type\n#              id\n#              name\n#              slug\n#              children {\n#                category_type\n#                id\n#                name\n#                slug\n#              }\n#            }\n#          }\n#        }\n#      }\n      breadcrumbs {\n        category_type\n        id\n        name\n        parent_id\n        parent_slug\n        slug\n      }\n      promo_banners {\n        id\n        image\n        name\n        category_ids\n        virtual_ids\n        type\n        sort_order\n        url\n        is_target_blank\n        analytics {\n          name\n          category\n          brand\n          type\n          start_date\n          end_date\n        }\n      }\n\n\n      dynamic_categories(from: 0, size: 9999) {\n        slug\n        name\n        id\n        category_type\n      }\n      filters {\n        facets {\n          key\n          total\n          filter {\n            id\n            name\n            display_title\n            is_list\n            is_main\n            text_filter\n            is_range\n            category_id\n            category_name\n            values {\n              slug\n              text\n              total\n            }\n          }\n        }\n      }\n      total\n      prices {\n        max\n        min\n      }\n      pricesFiltered {\n        max\n        min\n      }\n      products(attributeFilters: $attributes, from: $from, size: $size, sort: $sort, fieldFilters: $filters)  {\n        health_warning\n        limited_sale_qty\n        id\n        slug\n        name\n        name_highlight\n        article\n        main_article\n        main_article_slug\n        is_target\n        category_id\n        url\n        images\n        pick_up\n        rating\n        icons {\n          id\n          badge_bg_colors\n          rkn_icon\n          caption\n          image\n          type\n          is_only_for_sales\n          stores\n          caption_settings {\n            colors\n            text\n          }\n          stores\n          sort\n          image_png\n          image_svg\n          description\n          end_date\n          start_date\n          status\n        }\n        manufacturer {\n          id\n          image\n          name\n        }\n        packing {\n          size\n          type\n          pack_factors {\n            instamart\n          }\n        }\n        stocks {\n          value\n          text\n          eshop_availability\n          scale\n          prices_per_unit {\n            old_price\n            offline {\n              price\n              old_price\n              type\n              offline_discount\n              offline_promo\n            }\n            price\n            is_promo\n            levels {\n              count\n              price\n            }\n            online_levels {\n              count\n              price\n              discount\n            }\n            discount\n          }\n          prices {\n            price\n            is_promo\n            old_price\n            offline {\n              old_price\n              price\n              type\n              offline_discount\n              offline_promo\n            }\n            levels {\n              count\n              price\n            }\n            online_levels {\n              count\n              price\n              discount\n            }\n            discount\n          }\n        }\n      }\n    }\n  }\n',
    'variables': {
        'isShouldFetchOnlyProducts': True,
        'slug': 'molochnye-prodkuty-syry-i-yayca', # можно заменить на любую категорию
        'storeId': 15, # id города, (10 - Москва), можно спарсить любой город узнав id из POST запроса  https://api.metro-cc.ru/products-api/graph
        'sort': 'priceAsc',
        'size': 10000,
        'from': 0,
        'filters': [
            {
                'field': 'main_article',
                'value': '0',
            },
        ],
        'attributes': [],
        'in_stock': True,
        'eshop_order': True,
    },
}
response = requests.post('https://api.metro-cc.ru/products-api/graph', headers=headers, json=json_data)
r = json.loads(response.content)
products = r['data']['category']['products']


async def save(id, name, url, price, promo, price_on_promo, brand):
    async with aiofiles.open("Saint-Petersburg_metro_alkogolnaya-produkciya.csv", mode="a+", encoding="utf-8", newline="") as afp:
        writer = AsyncWriter(afp, dialect="unix")
        await writer.writerow([id, name, url, price, promo, price_on_promo, brand])


for product in products:
    id = product['id']
    name = product['name']
    url = 'https://online.metro-cc.ru' + product['url']
    price = product['stocks'][0]['prices']['price']
    promo = product['stocks'][0]['prices']['is_promo']
    price_on_promo = product['stocks'][0]['prices']['old_price']
    brand = product['manufacturer']['name']
    asyncio.run(save(id, name, url, price, promo, price_on_promo, brand))

    print('id:', product['id'])
    print('Наименование товара:', product['name'])
    print('Ссылка на товар:', product['url'])
    print('Цена без скидки:', product['stocks'][0]['prices']['price'])
    print('Наличие скидки:', product['stocks'][0]['prices']['is_promo'])
    print('Цена со скидкой:', product['stocks'][0]['prices']['old_price'])
    print('Брэнд:', product['manufacturer']['name'])
    print('-----------------------------------------------')
