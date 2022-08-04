import datetime
from explorer_s_common.utils import datetime_to_height, height_to_datetime
from flask import request, current_app
from explorer_s_common.third.bbhe_sdk import BbheEsBase, BbheBase
from explorer.models.message import MessagesAll, Messages, MigrateLog
from explorer.models.deal import Deal
from explorer.models.overview import Overview
from explorer.models.blocks import Tipset, Blocks
from explorer.models.wallets import Wallets, WalletRecords
from mongoengine.connection import get_db
from pymongo.operations import ReplaceOne, InsertOne
from explorer_s_common.utils import _d
from explorer_s_common import inner_server
from base.response import response_json


def sync_all_message():
    """
    获取数据
    """
    now = datetime.datetime.now()
    now_height = datetime_to_height(now)
    height = 31884
    log = MigrateLog.objects(version="s1").first()
    if log:
        height = int(log.message_content.get("height") or height) - 1
    # message_all_tmp = MessagesAll().to_mongo()
    # tmp = Messages().to_mongo()
    while height < now_height:
        message_list = []
        messages_list = []
        height += 1
        result = BbheEsBase().get_height_messages_back(height=height)
        messages = result.get('hits', [])
        if not messages:
            continue
        # 查询区块ID
        msg_cid_list = [per['_source'].get("msg_cid") for per in messages]
        block_result = BbheEsBase().get_block_by_message_ids(msg_cid_list)
        block_id_dict = {per['_source'].get("message"): per['_source'].get("block") for per in block_result.get('hits', [])}

        for per in messages:
            s = per['_source']
            message_list.append(
                ReplaceOne(
                {"msg_cid": s["msg_cid"]},
                {"msg_cid": s["msg_cid"],
                "msg_to": s["msg_to"],
                "msg_from": s["msg_from"],
                "msg_nonce": s["msg_nonce"],
                "msg_value": float(s["msg_value"]),
                "msg_gas_limit": int(s["msg_gas_limit"]),
                "msg_gas_fee_cap": int(s["msg_gas_fee_cap"]),
                "msg_gas_premium": int(s["msg_gas_premium"]),
                "msg_method": int(s["msg_method"]),
                "msg_method_name": s["msg_method_name"],
                "msg_params": s.get("msg_params"),
                "msg_return": s.get("msg_return"),
                "gascost_gas_used": float(s["gascost_gas_used"]),
                "gascost_base_fee_burn": float(s["gascost_base_fee_burn"]),
                "gascost_over_estimation_burn": float(s["gascost_over_estimation_burn"]),
                "gascost_miner_penalty": float(s["gascost_miner_penalty"]),
                "gascost_miner_tip": float(s["gascost_miner_tip"]),
                "gascost_refund": float(s["gascost_miner_tip"]),
                "gascost_total_cost": float(s["gascost_total_cost"]),
                "msgrct_exit_code": int(s["msgrct_exit_code"]),
                "base_fee": int(s.get("base_fee", 0) or 0),
                "sector_size": s.get("sector_size"),
                "sector_size_value": int(s.get("sector_size_value") or 0),
                "height": int(s["height"]),
                "height_time": height_to_datetime(s["height"]),
                "synced_at": s["synced_at"],
                "synced_at_str": s["synced_at_str"],
                "type": s.get("type")
                }, upsert=True))
            messages_list.append(
                ReplaceOne(
                {"msg_cid": s["msg_cid"]},
                {"msg_cid": s["msg_cid"],
                 "msg_to": s["msg_to"],
                 "msg_from": s["msg_from"],
                 "msg_value": float(s["msg_value"]),
                 "msg_method": int(s["msg_method"]),
                 "msg_method_name": s["msg_method_name"],
                 "sector_count": int(s.get("sector_count") or 1),
                 "sector_nums": s.get("sector_nums") or [s.get("sector_num")] if s.get("sector_num") else [],
                 "gascost_total_cost": float(s["gascost_total_cost"]),
                 "msgrct_exit_code": int(s["msgrct_exit_code"]),
                 "base_fee": int(s["base_fee"] or 0),
                 "sector_size_value": int(s.get("sector_size_value" or 0)),
                 "height": int(s["height"]),
                 "height_time": height_to_datetime(s["height"]),
                 "synced_at": s["synced_at"],
                 "synced_at_str": s["synced_at_str"],
                 "block_hash": block_id_dict.get(s["msg_cid"]),
                 "gascost_miner_penalty": float(s["gascost_miner_penalty"])
                 }, upsert=True)
            )
            # messages_list.append(messages_one)
        if messages_list:
            print(get_db("base").messages.bulk_write(messages_list).upserted_ids)
        if message_list:
            print(get_db("base").messages_all.bulk_write(message_list).upserted_ids)
        now = datetime.datetime.now()
        MigrateLog.objects(version="s1").update(message_content={"height": height,
                                                                 "datetime": now.strftime("%Y-%m-%d %H:%M:%S")},
                                                upsert=True)
    return response_json(True)


def sync_all_deal():
    """
    同步订单
    :return:
    """
    now = datetime.datetime.now()
    now_height = datetime_to_height(now)
    height = 820416
    log = MigrateLog.objects(version="s1").first()
    if log:
        height = int(log.deal_content.get("height") or height) - 1
    while height < now_height:
        deal_list = []
        height += 1
        result = inner_server.deal_all_list({"height": height})
        datas = result.get('data', [])
        if not datas:
            continue
        for s in datas:
            deal_list.append(
                ReplaceOne(
                    {"deal_id": int(s["deal_id"])},
                    {"deal_id": int(s["deal_id"]),
                     "piece_cid": s["piece_cid"],
                     "piece_size": float(s["piece_size"]),
                     "is_verified": bool(s["is_verified"]),
                     "client": s["client"],
                     "provider": s["provider"],
                     "start_epoch": int(s["start_epoch"]),
                     "end_epoch": int(s["end_epoch"]),
                     "storage_price_per_epoch": float(s["storage_price_per_epoch"]),
                     "provider_collateral": float(s.get("provider_collateral" or 0)),
                     "client_collateral": float(s.get("client_collateral" or 0)),
                     "msg_cid": s["msg_cid"],
                     "height": int(s["height"]),
                     "height_time": height_to_datetime(s["height"])
                     }, upsert=True)
            )
        if deal_list:
            print(get_db("base").deal.bulk_write(deal_list).upserted_ids)
        now = datetime.datetime.now()
        MigrateLog.objects(version="s1").update(deal_content={"height": height,
                                                              "datetime": now.strftime("%Y-%m-%d %H:%M:%S")},
                                                upsert=True)
    return response_json(True)


def sync_all_overview():
    """
    同步全网数据
    :return:
    """

    result = BbheBase().get_overview()
    if not result:
        return response_json(False)
    now = datetime.datetime.now()
    overview = {
        "power": float(result['data']['power']),
        "raw_power": float(result['data']['raw_power']),
        "active_miner_count": int(result['data']['active_miner']),
        "total_account": int(result['data']['total_account']),
        "pledge_per_sector": float(result['data']['pledge_per_sector']),
        "circulating_supply": float(result['data']['circulating_supply']),
        "burnt_supply": float(result['data']['burnt_supply']),
        "base_fee": float(result['data']['base_fee']),
        "msg_count": int(result['data']['msg_count']),
        "total_pledge": float(result['data']['total_pledge']),
        "reward": float(result['data']['reward']),
        "block_count": float(result['data']['block_count']),
        "block_reward": float(result['data']['block_reward']),
        "height": int(result['data']["height"]),
        "height_time": height_to_datetime(result['data']["height"]),
        "synced_at": now.timestamp(),
        "synced_at_str": now
    }
    Overview(**overview).save()
    now = datetime.datetime.now()
    MigrateLog.objects(version="s1").update(overview_content={"height": int(result['data']["height"]),
                                                             "datetime": now.strftime("%Y-%m-%d %H:%M:%S")},
                                            upsert=True)
    return response_json(True)


def add_tipset(height, blocks=[]):
    total_win_count = 0
    total_msg_count = 0
    total_reward = float(0)
    block_list = []
    for per in blocks:
        # 排除空块
        if per['minerReward'] == '0':
            continue
        total_win_count += int(per["win_count"])
        total_reward += float(per["reward"])
        total_msg_count += int(per["msg_count"])
        block_list.append(
            ReplaceOne(
                {"block_hash": per['block_hash']},
                {"block_hash": per['block_hash'],
                 "miner_no": per["miner_no"],
                 "msg_count": int(per["msg_count"]),
                 "win_count": int(per["win_count"]),
                 "reward": float(per["reward"]),
                 "penalty": float(per['penalty'] if len(per['penalty']) <= 40 else 0),
                 "height": int(height),
                 "height_time": height_to_datetime(height)
                 }, upsert=True)
        )
    tipsets = [ReplaceOne({"height": height},
                {"height": height,
                 "height_time": height_to_datetime(height),
                 "total_win_count": total_win_count,
                 "total_msg_count": total_msg_count,
                 "total_reward": total_reward,
                 }, upsert=True)]
    if block_list:
        print(get_db("base").blocks.bulk_write(block_list).upserted_ids)
    print(get_db("base").tipset.bulk_write(tipsets).upserted_ids)
    now = datetime.datetime.now()
    MigrateLog.objects(version="s1").update(block_content={"height": height,
                                                          "datetime": now.strftime("%Y-%m-%d %H:%M:%S")},
                                            upsert=True)


def sync_all_blocks():
    """
    同步区块信息
    :return:
    """
    result = BbheBase().get_blocks(page_size=100, height=982609)
    print(result)
    page_size = 100
    now = datetime.datetime.now()
    end_index = datetime_to_height(now)
    start_index = 3831
    tipset = Tipset.objects().order_by("-height").only("height").first()
    if tipset:
        start_index = min(tipset.height-(page_size+1), 0)

    log = MigrateLog.objects(version="s1").first()
    if log:
        start_index = min(int(log.block_content.get("height") or 1) - (page_size+1), 0)

    result = BbheBase().get_blocks(page_size=page_size, height=start_index + page_size)
    if not result.get('data', []):
        return response_json(True)

    while result.get('data', []) and start_index < end_index:
        i = 0
        temp_index = start_index
        for tipset in result['data']:
            height = tipset['height']
            while height != (temp_index + page_size - i):
                add_tipset(height=temp_index + page_size - i, blocks=[])
                start_index += 1
                i += 1
            add_tipset(height=height, blocks=tipset['blocks'])
            start_index += 1
            i += 1

        result = BbheBase().get_blocks(page_size=page_size, height=start_index + page_size)
    return response_json(True)


def sync_all_wallets():
    """
    同步钱包信息
    :return:
    """
    # now = datetime.datetime.now()
    # now_height = datetime_to_height(now)
    search_after = []
    log = MigrateLog.objects(version="s1").first()
    if log:
        search_after = log.deal_content.get("search_after", [])
    result = BbheEsBase().get_height_arockpool_wallet(search_after=search_after)
    datas = result.get('hits', [])
    while datas:
        wallets_list = []
        wallets_records = []
        for per in datas:
            s = per['_source']
            for m in ["owner", "poster", "worker"]:
                if s.get("{}_address".format(m)):
                    wallets_list.append(
                        ReplaceOne(
                            {"address": s.get("{}_address".format(m))},
                            {"address": s.get("{}_address".format(m)),
                             "value":  float(s.get("{}_balance_value".format(m)) or 0),
                             "id": s.get("{}_id".format(m)),
                             "wallet_type": "account",
                             "update_height": int(s["height"]),
                             "update_height_time": height_to_datetime(s["height"]),
                             "create_height": int(s["height"]),
                             "create_height_time": height_to_datetime(s["height"])
                             }, upsert=True)
                    )
                    wallets_records.append(
                        InsertOne(
                            {"address": s.get("{}_address".format(m)),
                             "value": float(s.get("{}_balance_value".format(m)) or 0),
                             "height": int(s["height"]),
                             "height_time": height_to_datetime(s["height"])
                             })
                    )
            search_after = per.get("sort", [])
        if wallets_list:
            print(get_db("base").wallets.bulk_write(wallets_list).upserted_ids)
        if wallets_list:
            print(get_db("base").wallet_records.bulk_write(wallets_records).inserted_count)
        now = datetime.datetime.now()
        MigrateLog.objects(version="s1").update(wallet_content={"search_after": search_after,
                                                               "datetime": now.strftime("%Y-%m-%d %H:%M:%S")},
                                                upsert=True)
        result = BbheEsBase().get_height_arockpool_wallet(search_after=search_after)
        datas = result.get('hits', [])
    return response_json(True)


def sync_all_miner():
    now = datetime.datetime.now()
    page_index = 0
    page_size = 100
    result = BbheBase().get_active_miners(page_index=page_index, page_size=page_size)
    if not result:
        return response_json(True)
    while result.get('data', []):
        miner_list = []
        for per in result.get('data', []):
            miner_list.append(
                ReplaceOne(
                    {"miner_no": per['miner_no']},
                    {"miner_no": per['miner_no'],
                     "address": per["miner_address"],
                     "account_type": per.get("account_type"),
                     "owner_id": per.get("owner"),
                     "owner_address": per.get("owner_address"),
                     "worker_id": per.get("worker"),
                     "worker_address": per.get("worker_address"),
                     "poster_id": per.get("poster_id"),
                     "post_address": per.get("post_address"),
                     "total_balance": float(per["balance"]),
                     "available_balance": float(per["available_balance"]),
                     "pledge_balance": float(per["pledge_balance"]),
                     "initial_pledge_balance": float(per["initial_pledge"]),
                     "locked_balance": float(per["locked_funds"]),
                     "actual_power": float(per["power"]),
                     "raw_power": float(per["raw_power"]),
                     "sector_all": int(per["total_sector"]),
                     "sector_effect": int(per["active_sector"]),
                     "sector_faults": int(per["faulty_sector"]),
                     "sector_recovering": int(per["recovering_sector"]),
                     "sector_size": int(per["sector_size"]),
                     "peer_id": per["peer_id"],
                     "msg_count":per["msg_count"],
                     "lucky": float(per["lucky"]),
                     "synced_at": now.timestamp(),
                     "synced_at_str":now
                     }, upsert=True)
            )
        if miner_list:
            print(get_db("base").miners.bulk_write(miner_list).upserted_ids)
        page_index += 1
        result = BbheBase().get_active_miners(page_index=page_index, page_size=page_size)
    return response_json(True)