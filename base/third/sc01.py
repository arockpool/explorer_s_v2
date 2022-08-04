import re
import time, json
from decimal import Decimal

import requests
# from django.db import transaction

# from tools.models import *

proxy = "http://squid:squid666@127.0.0.1:8888"
proxies = {
    'http': proxy, 'https': proxy
}


def initial_data():
    pass


def remove_bracket(data: str):
    ret = re.search(r'<(.*?)>(.*?)<(.*?)>', data)
    if ret:
        return ret.groups()[1]
    return data


def get_details(data):
    github_account = re.search(
        r'<a class="author Link--primary css-truncate-target width-fit" show_full_name="false" data-hovercard-type="user" data-hovercard-url="/users/(.*?)/hovercard" data-octo-click="hovercard-link-click" data-octo-dimensions="link_type:self" href="/(.*?)">(.*?)</a>',
        data).groups()[0]
    display_name = (re.search(r'<li>Name: (.*?)</li>', data) or re.search(r'<li>Name:(.*?)</li>', data) or re.search(
        r'<li><p>Name:(.*?)</p></li>', data) or re.search(r'<li><p>Name: (.*?)</p></li>', data) or re.search(
        r'<li>\n<p>Name:(.*?)</p>\n</li>', data) or re.search(
        r'<li>\n<p dir="auto">Name:(.*?)</p>\n</li>', data) or re.search(r'<li>\n<p dir="auto">Name: (.*?)</p>\n</li>', data)).groups()[0].strip()
    org = (re.search(r'<li>Affiliated Organization: (.*?)</li>', data) or re.search(r'<li>Affiliated Organization:(.*?)</li>', data) or re.search(
        r'<li><p>Affiliated Organization:(.*?)</p></li>', data) or re.search(r'<li><p>Affiliated Organization: (.*?)</p></li>', data) or re.search(
        r'<li>\n<p>Affiliated Organization:(.*?)</p>\n</li>', data) or re.search(
        r'<li><p dir="auto">Affiliated Organization:(.*?)</p></li>', data) or re.search(r'<li><p dir="auto">Affiliated Organization: (.*?)</p></li>', data))
    if org:
        org = org.groups()[0].strip()
    else:
        org = ""
    media = (re.search(r'<li>Website(.*?): (.*)</li>', data) or re.search(r'<li>Website(.*?):(.*)</li>',
                                                                          data) or re.search(
        r'<li>'
        r'n<p>Website(.*?): (.*)<>\n</li>', data) or re.search(r'<li>\n<p>Website(.*?):(.*)<>\n</li>',
                                                                    data) or re.search(
        r'<li>\n<p>Website(.*?): (.*?)</p>\n</li>', data) or re.search(r'<li>\n<p>Website(.*?):(.*?)</p>\n</li>', data,
                                                                       re.DOTALL) or
         re.search(r'<li>\n<p dir="auto">Website(.*?): (.*?)</p>\n</li>', data) or re.search(r'<li>\n<p dir="auto">Website(.*?):(.*?)</p>\n</li>',
                                                                            data, re.DOTALL)
             ).groups()[1].strip()
    address = (re.search(r'<blockquote>\n(.*?)<p>(.*?)</p>\n</blockquote>\n<h4>Datacap Allocated</h4>', data) or
               re.search(r'<blockquote>\n(.*?)<p dir="auto">(.*?)</p>\n</blockquote>\n<h4 dir="auto">Datacap Allocated</h4>', data)).groups()[
        1].strip()
    use_case = (re.search(r'<li>Use case(.*?): (.*?)</li>', data, re.S | re.M) or re.search(
        r'<li>\n<p>Use case(.*?): (.*?)</p>\n</li>', data, re.S | re.M)  or re.search(
                r'<li>\n<p dir="auto">Use case(.*?): (.*?)</p>\n</li>', data, re.S | re.M)
                ).groups()[1].strip()
    return {'github_account': remove_bracket(github_account), 'display_name': remove_bracket(display_name),
            'org': remove_bracket(org),
            'media': remove_bracket(media), 'address': remove_bracket(address), 'use_case': remove_bracket(use_case)}


# @transaction.atomic()
def initial_notaries_data():
    res = requests.get(url='https://github.com/filecoin-project/notary-governance/tree/main/notaries#overview',
                       proxies=proxies)
    data = res.text
    table_data = re.search(r'<table>(.*)</table>', data, re.M | re.S).group()
    tbody_data = re.search(r'<tbody>(.*)</tbody>', table_data, re.M | re.S).group()
    tr_data = re.findall(r'<tr>(.*?)</tr>', tbody_data, re.M | re.S)
    notaries_dict = {}
    for single_data in tr_data:
        sda = re.search(r'<td>(.*?)</td>\n<td>(.*?)</td>\n<td><.*?>(.*?)<.*?></td>\n<td>(.*?)</td>', single_data,
                        re.M | re.S)
        value, unit = re.search(r'(\d*)(\w*)', sda.group(4)).groups()
        if unit.endswith('GiB'):
            datacap_granted = Decimal(value) * (1024 ** 3)
        elif unit.endswith('TiB'):
            datacap_granted = Decimal(value) * (1024 ** 4)
        elif unit.endswith('PiB'):
            datacap_granted = Decimal(value) * (1024 ** 5)
        notaries_dict.setdefault(sda.group(2), {'region': sda.group(1), 'application_link': sda.group(3),
                                                'latest_datacap_grant': datacap_granted})
    result = []
    for k, v in notaries_dict.items():
        time.sleep(0.1)
        # try:
        data = requests.get(v.get('application_link'), timeout=10, proxies=proxies).text
        v.update(get_details(data))
        # except Exception as e:
        #     print(data)
        #     print(e)
        cd = {
            'name': k,
            'org': v.get('org'),
            'github_account': v.get('github_account'),
            'address': v.get('address'),
            'id_address': get_id_address(v.get('address')),
            'media': v.get('media'),
            'use_case': v.get('use_case'),
            'region': v.get('region'),
            'application_link': v.get('application_link'),
            'granted_allowance': v.get('latest_datacap_grant'),
            'granted_date': '2021-05-08',
        }
        result.append(cd)
        # print(cd)
        # Notaries.objects.update_or_create(**cd)
    return result


def get_id_address(address: str):
    get_id_address_url = 'https://api.filscout.com/api/v1/actor/byaddress/' + address
    data = requests.get(get_id_address_url).json().get('data')
    actor = data.get('actor') or data.get('actorMulti')
    return actor.get('idAddress') if actor else '--'


# 'Holon Innovations': {'region': 'Oceania',
# 'apllication_link': 'https://github.com/filecoin-project/notary-governance/issues/130',
# 'latest_datacap_grant': Decimal('109951162777600'),
# 'github_account': 'MegTei',
# 'display_name': 'Holon Innovations',
# 'media': 'https://holon.investments/',
# 'address': 'f1ystxl2ootvpirpa7ebgwl7vlhwkbx2r4zjxwe5i',
# 'use_case': 'https://docs.google.com/document/d/1_Z-fe-e5zOXdpvlumYsTdI1Lr7l1GF8bmpw59tXAxxA/edit#heading=h.2b63d2ru17hx'}


# for i in alias_tuple:
#     item_list.append(ProductSheet(
#         category='次数计费',
#         category_en = 'request times',
#         type=i[1],
#         type_en=i[4],
#         alias=i[0],
#         alias_en=i[3],
#         service_area='中国大陆通用',
#         service_area_en='China',
#         data_size=None,
#         unit_price=i[2],
#         period=None,
#         code=uuid4().hex,
#         unit=None,
#         group=None))
#
# ProductSheet.objects.bulk_create(item_list)

def add_new_github(account, account_dict):
    if account and account not in account_dict:
        account_dict.update({account: account})
    return account_dict


# def github_lists():
#     nts = Notaries.objects.all()
#     for nt in nts:
#         # print(nt.id,nt.github_account)
#         nt.github_accounts_dict = add_new_github(nt.github_account, nt.github_accounts_dict)
#         nt.save()

# ((6,'philippbanhardt'),())