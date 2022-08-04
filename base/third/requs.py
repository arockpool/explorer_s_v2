import json
import re
from datetime import datetime
from decimal import Decimal

import requests
# from django.db import transaction

# from tools.models import RequestAllowanceRecord

url = 'https://api.github.com/repos/filecoin-project/filecoin-plus-client-onboarding/issues'

body_list = []

proxy = "http://squid:squid666@127.0.0.1:8888"
proxies = {
    'http': proxy, 'https': proxy
}
state_choices = ('closed',)
# state_choices = ('closed', 'open')


def request_data(url, body, headers, repeat_time=10):
    initial_time = 1
    while initial_time < repeat_time:
        print(f'尝试第{initial_time}次')
        try:
            req = requests.post(url, headers=headers, data=body, timeout=30)
        except Exception as e:
            print(e)
            initial_time += 1
            continue
        if req.status_code == 200:
            return req.json().get('data')
        initial_time += 1


# state_choices = ('closed',)
# @transaction.atomic()
def initial_req():
    a = []
    b = []
    data_list = []
    for choice_ in state_choices:
        initial_page = 1
        while True:
            headers = {"Authorization": 'token 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'}
            req = requests.get(url, headers=headers, proxies=proxies,
                               params={'state': choice_, 'per_page': '100', 'page': str(initial_page)}).json()
            if not req:
                break
            for data in req:
                assignor = data.get('user').get('login')
                issue_state = data.get('state')
                labels = data.get('labels')
                created_at = data.get('created_at')
                closed_at = data.get('closed_at')
                updated_at = data.get('updated_at')
                comments_url = data.get('comments_url')
                issue_body = data.get('body')
                assignee = data.get("assignee").get('login') if data.get("assignee") else None
                title = data.get("title")
                if re.search(r'test', title, re.I):
                    continue
                msg_cid = None
                allocated_address = None
                allocated_datacap = None
                flag = True

                # print(assignor, issue_state, comments_url, assignee)
                # ((0, '已关闭未分配'), (1, '待处理'), (2, '已分配'))
                status = 0 if state_choices == 'closed' else 1

                # print(issue_body)
                # print(data.get('url'))
                if not issue_body:
                    continue
                if not (re.search(r'name: (.*?)\n', issue_body, re.M | re.I) or re.search(r'name:(.*?)\n', issue_body,
                                                                                          re.M | re.I)):
                    continue
                if (re.search(r'name: (.*?)\n', issue_body, re.M | re.I) or re.search(r'name:(.*?)\n', issue_body,
                                                                                      re.M | re.I)).groups()[0] == '\r':
                    continue
                name = (re.search(r'name: (.*?)\n', issue_body, re.M | re.I) or re.search(r'name:(.*?)\n', issue_body,
                                                                                          re.M | re.I)).groups()[0]
                media = (re.search(r'Website(.*?): (.*?)\n', issue_body, re.M | re.I) or re.search(r'Website(.*?):(.*?)\n',
                                                                                                   issue_body,
                                                                                                   re.M | re.I)).groups()[1]
                apply_address = re.search(r'(f1\w{5,}|f2\w{5,}|f3\w{5,})', issue_body, re.M | re.I).groups(
                    [0]) if re.search(r'(f1\w{5,}|f2\w{5,}|f3\w{5,})', issue_body, re.M | re.I) else None
                region = re.search(r'Region: (.*?)\n', issue_body, re.M | re.I).groups()[0] if re.search(r'Region: (.*?)\n',
                                                                                                         issue_body,
                                                                                                         re.M | re.I) else None
                request_datacap = (re.search(r'(DataCap.*?): (\d+.*)', issue_body) or re.search(r'(DataCap.*?):(\d+.*)',
                                                                                                issue_body) or re.search(
                    r'(DataCap.*?):(.{3,})', issue_body)).groups()[1]
                for label in labels:
                    # print(issue_state, comments_url, initial_page)
                    if 'Granted' in label.get('name'):
                        a.append(comments_url)
                        ret1 = requests.get(url=comments_url, proxies=proxies, headers=headers).json()
                        for ret in ret1:
                            body_data = ret.get('body')
                            if 'Datacap Allocated' in body_data:
                                # print(body_data)
                                b.append(comments_url)
                                msg_cid = list(set(re.findall(r'(bafy\w*)', body_data, re.M)))
                                allocated_address = (re.search(r'> (f\w{7,})', body_data) or
                                                     re.search(r'>(f\w{7,})', body_data) or
                                                     re.search(r'\[(f1\w{5,}|f2\w{5,}|f3\w{5,})\]', body_data)).groups()[0]
                                allocated_datacap = \
                                (re.search(r'> (\d+.*)', body_data) or re.search(r'>(\d+.*)', body_data) or re.search(
                                    r'DataCap Requested:  (\d+.*)', issue_body) or re.search(r'DataCap Requested:(\d+.*)', issue_body))
                                if allocated_datacap:
                                    allocated_datacap = allocated_datacap.groups()[0]
                                else:
                                    allocated_datacap = 0
                                body_list.append(ret.get('body'))
                                if flag:
                                    # print(request_datacap)
                                    if request_datacap and request_datacap.strip() != 'None':
                                        request_datacap = request_datacap.strip()
                                        if not re.match(r'^\d', request_datacap):
                                            request_datacap = re.search(r'(\d.*\w+)', request_datacap).groups()[0].strip()
                                        origin_data = (re.search(r'(.*?)(T|G|P|t|g|p|K|k|M|m)', request_datacap))
                                        num = origin_data.groups()[0].strip()
                                        unit = origin_data.groups()[1].strip()
                                        # print('>>>>>>>>>>>>>>>',num,unit,unit.startswith('T'),unit.startswith('t'))
                                        if unit.startswith('P') or unit.startswith('p'):
                                            request_datacap = Decimal(num) * (1024 ** 5)
                                        elif unit.startswith('T') or unit.startswith('t'):
                                            request_datacap = Decimal(num) * (1024 ** 4)
                                        elif unit.startswith('G') or unit.startswith('g'):
                                            request_datacap = Decimal(num) * (1024 ** 3)
                                        elif unit.startswith('M') or unit.startswith('m'):
                                            request_datacap = Decimal(num) * (1024 ** 2)
                                        elif unit.startswith('K') or unit.startswith('k'):
                                            request_datacap = Decimal(num) * (1024 ** 1)
                                    else:
                                        request_datacap = None
                                    # print(allocated_datacap)
                                    if allocated_datacap and allocated_datacap.strip() != 'None':
                                        allocated_datacap = allocated_datacap.strip()
                                        if not re.match(r'^\d', allocated_datacap):
                                            allocated_datacap = re.search(r'(\d.*\w+)', allocated_datacap).groups()[
                                                0].strip()
                                        origin_data = re.search(r'(.*?)(T|G|P|t|g|p|K|k|M|m)', allocated_datacap)
                                        num = origin_data.groups()[0].strip()
                                        unit = origin_data.groups()[1].strip()
                                        # print('>>>>>>>>>>>>>>>',num,unit,unit.startswith('T'),unit.startswith('t'))
                                        if unit.startswith('P') or unit.startswith('p'):
                                            allocated_datacap = Decimal(num) * (1024 ** 5)
                                        elif unit.startswith('T') or unit.startswith('t'):
                                            allocated_datacap = Decimal(num) * (1024 ** 4)
                                        elif unit.startswith('G') or unit.startswith('g'):
                                            allocated_datacap = Decimal(num) * (1024 ** 3)
                                        elif unit.startswith('M') or unit.startswith('m'):
                                            allocated_datacap = Decimal(num) * (1024 ** 2)
                                        elif unit.startswith('K') or unit.startswith('k'):
                                            allocated_datacap = Decimal(num) * (1024 ** 1)
                                    else:
                                        allocated_datacap = None
                                    status=2
                                    create_dict={
                                        'name': name,
                                        'media': media,
                                        'region': region,
                                        'request_datacap': request_datacap if request_datacap else 0,
                                        'assignor': assignor,
                                        'created_at': datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%SZ'),
                                        'closed_at': datetime.strptime(closed_at, '%Y-%m-%dT%H:%M:%SZ'),
                                        'updated_at': datetime.strptime(updated_at, '%Y-%m-%dT%H:%M:%SZ'),
                                        'comments_url': comments_url,
                                        'assignee': assignee,
                                        'status': status,
                                        'msg_cid': msg_cid[0] if msg_cid else msg_cid,
                                        'apply_address': apply_address[0] if apply_address else apply_address,
                                        'allocated_address': allocated_address,
                                        'allocated_datacap': allocated_datacap if allocated_datacap else Decimal(0),
                                        'distribute_date': datetime.strptime(closed_at, '%Y-%m-%dT%H:%M:%SZ').date() if status==2 else None
                                    }
                                    # if not RequestAllowanceRecord.objects.filter(**create_dict).count():
                                    #     RequestAllowanceRecord.objects.create(**create_dict)
                                    # else:
                                    #     break
                                    data_list.append(create_dict)
                                    flag = False
                # if flag:
                #     print(request_datacap, type(request_datacap))
                #     if request_datacap and request_datacap.strip() != 'None':
                #         request_datacap = request_datacap.strip()
                #         if not re.match(r'^\d', request_datacap):
                #             request_datacap = re.search(r'(\d.*\w+)', request_datacap).groups()[0].strip()
                #         origin_data = re.search(r'(.*?)(T|G|P|t|g|p|K|k|M|m)', request_datacap)
                #         num = origin_data.groups()[0].strip()
                #         unit = origin_data.groups()[1].strip()
                #         print('>>>>>>>>>>>>>>>',num,unit,unit.startswith('T'),unit.startswith('t'))
                #         if unit.startswith('P') or unit.startswith('p'):
                #             request_datacap = Decimal(num) * (1024 ** 5)
                #         elif unit.startswith('T') or unit.startswith('t'):
                #             request_datacap = Decimal(num) * (1024 ** 4)
                #         elif unit.startswith('G') or unit.startswith('g'):
                #             request_datacap = Decimal(num) * (1024 ** 3)
                #         elif unit.startswith('M') or unit.startswith('m'):
                #             request_datacap = Decimal(num) * (1024 ** 2)
                #         elif unit.startswith('K') or unit.startswith('k'):
                #             request_datacap = Decimal(num) * (1024 ** 1)
                #     else:
                #         request_datacap = None
                #     print(allocated_datacap)
                #     if allocated_datacap and allocated_datacap.strip() != 'None':
                #         allocated_datacap = allocated_datacap.strip()
                #         if not re.match(r'^\d', allocated_datacap):
                #             allocated_datacap = re.search(r'(\d.*\w+)', allocated_datacap).groups()[0].strip()
                #         origin_data = re.search(r'(.*?)(T|G|P|t|g|p|K|k|M|m)', allocated_datacap)
                #         num = origin_data.groups()[0].strip()
                #         unit = origin_data.groups()[1].strip()
                #         print('>>>>>>>>>>>>>>>',num,unit,unit.startswith('T'),unit.startswith('t'))
                #         if unit.startswith('P') or unit.startswith('p'):
                #             allocated_datacap = Decimal(num) * (1024 ** 5)
                #         elif unit.startswith('T') or unit.startswith('t'):
                #             allocated_datacap = Decimal(num) * (1024 ** 4)
                #         elif unit.startswith('G') or unit.startswith('g'):
                #             allocated_datacap = Decimal(num) * (1024 ** 3)
                #         elif unit.startswith('M') or unit.startswith('m'):
                #             allocated_datacap = Decimal(num) * (1024 ** 2)
                #         elif unit.startswith('K') or unit.startswith('k'):
                #             allocated_datacap = Decimal(num) * (1024 ** 1)
                #     else:
                #         allocated_datacap = None
                #     create_dict={
                #         'name': name,
                #         'media': media,
                #         'region': region,
                #         'request_datacap': request_datacap if request_datacap else 0,
                #         'assignor': assignor,
                #         'created_at': datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%SZ'),
                #         'closed_at': datetime.strptime(closed_at, '%Y-%m-%dT%H:%M:%SZ') if closed_at else None,
                #         'updated_at': datetime.strptime(updated_at, '%Y-%m-%dT%H:%M:%SZ'),
                #         'comments_url': comments_url,
                #         'assignee': assignee,
                #         'status': status,
                #         'msg_cid': msg_cid[0] if msg_cid else msg_cid,
                #         'apply_address': apply_address[0] if apply_address else apply_address,
                #         'allocated_address': allocated_address,
                #         'allocated_datacap': allocated_datacap if allocated_datacap else 0,
                #         'distribute_date': datetime.strptime(closed_at, '%Y-%m-%dT%H:%M:%SZ').date() if status==2 else None
                #     }
                #     # if not RequestAllowanceRecord.objects.filter(**create_dict).count():
                #     #     RequestAllowanceRecord.objects.create(**create_dict)
                #     # else:
                #     #     break
                #     flag = False
            initial_page += 1
    return data_list
# @transaction.atomic()
# def initial_req():
#     a = []
#     b = []
#     data_list = []
#     for choice_ in state_choices:
#         initial_page = 1
#         while True:
#             headers = {"Authorization": 'token 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'}
#             req = requests.get(url, headers=headers, proxies=proxies,
#                                params={'state': choice_, 'per_page': '100', 'page': str(initial_page)}).json()
#             if not req:
#                 break
#             for data in req:
#                 assignor = data.get('user').get('login')
#                 issue_state = data.get('state')
#                 labels = data.get('labels')
#                 created_at = data.get('created_at')
#                 closed_at = data.get('closed_at')
#                 updated_at = data.get('updated_at')
#                 comments_url = data.get('comments_url')
#                 issue_body = data.get('body')
#                 assignee = data.get("assignee").get('login') if data.get("assignee") else None
#                 title = data.get("title")
#                 if re.search(r'test', title, re.I):
#                     continue
#                 msg_cid = None
#                 allocated_address = None
#                 allocated_datacap = None
#                 flag = True
#
#                 # print(assignor, issue_state, comments_url, assignee)
#                 # ((0, '已关闭未分配'), (1, '待处理'), (2, '已分配'))
#                 status = 0 if state_choices == 'closed' else 1
#
#                 print(issue_body)
#                 print(data.get('url'))
#                 if not issue_body:
#                     continue
#                 if not (re.search(r'name: (.*?)\n', issue_body, re.M | re.I) or re.search(r'name:(.*?)\n', issue_body,
#                                                                                           re.M | re.I)):
#                     continue
#                 if (re.search(r'name: (.*?)\n', issue_body, re.M | re.I) or re.search(r'name:(.*?)\n', issue_body,
#                                                                                       re.M | re.I)).groups()[0] == '\r':
#                     continue
#                 name = (re.search(r'name: (.*?)\n', issue_body, re.M | re.I) or re.search(r'name:(.*?)\n', issue_body,
#                                                                                           re.M | re.I)).groups()[0]
#                 media = (re.search(r'Website(.*?): (.*?)\n', issue_body, re.M | re.I) or re.search(r'Website(.*?):(.*?)\n',
#                                                                                                    issue_body,
#                                                                                                    re.M | re.I)).groups()[1]
#                 apply_address = re.search(r'(f1\w{5,}|f2\w{5,}|f3\w{5,})', issue_body, re.M | re.I).groups(
#                     [0]) if re.search(r'(f1\w{5,}|f2\w{5,}|f3\w{5,})', issue_body, re.M | re.I) else None
#                 region = re.search(r'Region: (.*?)\n', issue_body, re.M | re.I).groups()[0] if re.search(r'Region: (.*?)\n',
#                                                                                                          issue_body,
#                                                                                                          re.M | re.I) else None
#                 request_datacap = (re.search(r'(DataCap.*?): (\w{2,})', issue_body) or re.search(r'(DataCap.*?):(\w{2,})',
#                                                                                                  issue_body) or re.search(
#                     r'(DataCap.*?):(.{3,})', issue_body)).groups()[1]
#                 for label in labels:
#                     print(issue_state, comments_url, initial_page)
#                     if 'Granted' in label.get('name'):
#                         a.append(comments_url)
#                         ret1 = requests.get(url=comments_url, proxies=proxies, headers=headers).json()
#                         for ret in ret1:
#                             body_data = ret.get('body')
#                             if 'Datacap Allocated' in body_data:
#                                 b.append(comments_url)
#                                 msg_cid = list(set(re.findall(r'(bafy\w*)', body_data, re.M)))
#                                 allocated_address = (re.search(r'> (f\w{7,})', body_data) or re.search(
#                                     r'\[(f1\w{5,}|f2\w{5,}|f3\w{5,})\]', body_data)).groups()[0]
#                                 print(body_data)
#                                 allocated_datacap = \
#                                     (re.search(r'> (.*?iB)', issue_body) or re.search(r'>(.*?iB)', body_data) or re.search(
#                                         r'> (.*?B)', issue_body) or re.search(r'>(.*?B)', body_data)).groups()[0]
#                                 body_list.append(ret.get('body'))
#                                 if flag:
#                                                           print(request_datacap)
#                                 if request_datacap and request_datacap.strip() != 'None':
#                                     request_datacap = request_datacap.strip()
#                                     if not re.match(r'^\d', request_datacap):
#                                         request_datacap = re.search(r'(\d.*\w+)', request_datacap).groups()[0].strip()
#                                     origin_data = (re.search(r'(.*?)(T|G|P|t|g|p|K|k|M|m)', request_datacap))
#                                     num = origin_data.groups()[0].strip()
#                                     unit = origin_data.groups()[1].strip()
#                                     print('>>>>>>>>>>>>>>>',num,unit,unit.startswith('T'),unit.startswith('t'))
#                                     if unit.startswith('P') or unit.startswith('p'):
#                                         request_datacap = Decimal(num) * (1024 ** 5)
#                                     elif unit.startswith('T') or unit.startswith('t'):
#                                         request_datacap = Decimal(num) * (1024 ** 4)
#                                     elif unit.startswith('G') or unit.startswith('g'):
#                                         request_datacap = Decimal(num) * (1024 ** 3)
#                                     elif unit.startswith('M') or unit.startswith('m'):
#                                         request_datacap = Decimal(num) * (1024 ** 2)
#                                     elif unit.startswith('K') or unit.startswith('k'):
#                                         request_datacap = Decimal(num) * (1024 ** 1)
#                                 else:
#                                     request_datacap = None
#                                 print(allocated_datacap)
#                                 if allocated_datacap and allocated_datacap.strip() != 'None':
#                                     allocated_datacap = allocated_datacap.strip()
#                                     if not re.match(r'^\d', allocated_datacap):
#                                         allocated_datacap = re.search(r'(\d.*\w+)', allocated_datacap).groups()[
#                                             0].strip()
#                                     origin_data = re.search(r'(.*?)(T|G|P|t|g|p|K|k|M|m)', allocated_datacap)
#                                     num = origin_data.groups()[0].strip()
#                                     unit = origin_data.groups()[1].strip()
#                                     print('>>>>>>>>>>>>>>>',num,unit,unit.startswith('T'),unit.startswith('t'))
#                                     if unit.startswith('P') or unit.startswith('p'):
#                                         allocated_datacap = Decimal(num) * (1024 ** 5)
#                                     elif unit.startswith('T') or unit.startswith('t'):
#                                         allocated_datacap = Decimal(num) * (1024 ** 4)
#                                     elif unit.startswith('G') or unit.startswith('g'):
#                                         allocated_datacap = Decimal(num) * (1024 ** 3)
#                                     elif unit.startswith('M') or unit.startswith('m'):
#                                         allocated_datacap = Decimal(num) * (1024 ** 2)
#                                     elif unit.startswith('K') or unit.startswith('k'):
#                                         allocated_datacap = Decimal(num) * (1024 ** 1)
#                                 else:
#                                     allocated_datacap = None
#                                     status = 2
#                                     create_dict ={
#                                         'name': name,
#                                         'media': media,
#                                         'region': region,
#                                         'request_datacap': request_datacap,
#                                         'assignor': assignor,
#                                         'created_at': datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%SZ'),
#                                         'closed_at': datetime.strptime(closed_at, '%Y-%m-%dT%H:%M:%SZ') if closed_at else None,
#                                         'updated_at': datetime.strptime(updated_at, '%Y-%m-%dT%H:%M:%SZ'),
#                                         'distribute_date': datetime.strptime(closed_at, '%Y-%m-%dT%H:%M:%SZ').date() if status==2 else None,
#                                         'comments_url': comments_url,
#                                         'assignee': assignee,
#                                         'status': status,
#                                         'msg_cid': msg_cid[0] if msg_cid else msg_cid,
#                                         'apply_address': apply_address[0] if apply_address else apply_address,
#                                         'allocated_address': allocated_address,
#                                         'allocated_datacap': allocated_datacap,
#                                     }
#                                     if not RequestAllowanceRecord.objects.filter(**create_dict).count():
#                                         RequestAllowanceRecord.objects.create(**create_dict)
#                                     else:
#                                         break
#                                     flag = False
#                 if flag:
#                     print(request_datacap)
#                     if request_datacap and request_datacap.strip() != 'None':
#                         request_datacap = request_datacap.strip()
#                         if not re.match(r'^\d', request_datacap):
#                             request_datacap = re.search(r'(\d.*\w+)', request_datacap).groups()[0].strip()
#                         origin_data = (re.search(r'(.*?)(T|G|P|t|g|p|K|k|M|m)', request_datacap))
#                         num = origin_data.groups()[0].strip()
#                         unit = origin_data.groups()[1].strip()
#                         print('>>>>>>>>>>>>>>>',num,unit,unit.startswith('T'),unit.startswith('t'))
#                         if unit.startswith('P') or unit.startswith('p'):
#                             request_datacap = Decimal(num) * (1024 ** 5)
#                         elif unit.startswith('T') or unit.startswith('t'):
#                             request_datacap = Decimal(num) * (1024 ** 4)
#                         elif unit.startswith('G') or unit.startswith('g'):
#                             request_datacap = Decimal(num) * (1024 ** 3)
#                         elif unit.startswith('M') or unit.startswith('m'):
#                             request_datacap = Decimal(num) * (1024 ** 2)
#                         elif unit.startswith('K') or unit.startswith('k'):
#                             request_datacap = Decimal(num) * (1024 ** 1)
#                     else:
#                         request_datacap = None
#                     print(allocated_datacap)
#                     if allocated_datacap and allocated_datacap.strip() != 'None':
#                         allocated_datacap = allocated_datacap.strip()
#                         if not re.match(r'^\d', allocated_datacap):
#                             allocated_datacap = re.search(r'(\d.*\w+)', allocated_datacap).groups()[
#                                 0].strip()
#                         origin_data = re.search(r'(.*?)(T|G|P|t|g|p|K|k|M|m)', allocated_datacap)
#                         num = origin_data.groups()[0].strip()
#                         unit = origin_data.groups()[1].strip()
#                         print('>>>>>>>>>>>>>>>',num,unit,unit.startswith('T'),unit.startswith('t'))
#                         if unit.startswith('P') or unit.startswith('p'):
#                             allocated_datacap = Decimal(num) * (1024 ** 5)
#                         elif unit.startswith('T') or unit.startswith('t'):
#                             allocated_datacap = Decimal(num) * (1024 ** 4)
#                         elif unit.startswith('G') or unit.startswith('g'):
#                             allocated_datacap = Decimal(num) * (1024 ** 3)
#                         elif unit.startswith('M') or unit.startswith('m'):
#                             allocated_datacap = Decimal(num) * (1024 ** 2)
#                         elif unit.startswith('K') or unit.startswith('k'):
#                             allocated_datacap = Decimal(num) * (1024 ** 1)
#                     else:
#                         allocated_datacap = None
#                     create_dict={
#                         'name': name,
#                         'media': media,
#                         'region': region,
#                         'request_datacap': request_datacap,
#                         'assignor': assignor,
#                         'created_at': datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%SZ'),
#                         'closed_at': datetime.strptime(closed_at, '%Y-%m-%dT%H:%M:%SZ') if closed_at else None,
#                         'updated_at': datetime.strptime(updated_at, '%Y-%m-%dT%H:%M:%SZ'),
#                         'distribute_date': None,
#                         'comments_url': comments_url,
#                         'assignee': assignee,
#                         'status': status,
#                         'msg_cid': msg_cid[0] if msg_cid else msg_cid,
#                         'apply_address': apply_address[0] if apply_address else apply_address,
#                         'allocated_address': allocated_address,
#                         'allocated_datacap': allocated_datacap,
#                     }
#                     if not RequestAllowanceRecord.objects.filter(**create_dict).count():
#                         RequestAllowanceRecord.objects.create(**create_dict)
#                     else:
#                         break
#                     flag = False
#             initial_page += 1
# with open('datalist.txt', 'w') as f:
#     for b in data_list:
#         if b:
#             # print(b)
#             # print(len(data_list))
#             f.write(json.dumps(b, ensure_ascii=True))
#             f.write('\n')
#             f.write('\n')
#             f.write('\n')
# if not RequestAllowanceRecord.objects.filter(**create_dict).count():
#     RequestAllowanceRecord.objects.create(**create_dict)
# else:
#     break