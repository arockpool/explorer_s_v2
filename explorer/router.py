from .api import message, miner, overview, block, wallets, deal, common, datacap, inner_api,tool

url_map = {
    '/health': [lambda *args, **kwargs: 'ok', ['GET']],
    '/sync': {
        '/sync_tipset_gas': [message.sync_tipset_gas, ['POST']],
        '/sync_messages_stat': [message.sync_messages_stat, ['POST']],
        '/sync_miner_total_blocks': [miner.sync_miner_total_blocks, ['POST']],
        '/sync_miner_stat': [miner.sync_miner_stat, ['POST']],
        '/sync_miner_day': [miner.sync_miner_day, ['POST']],
        '/sync_miner_day_block': [miner.sync_miner_day_block, ['POST']],
        '/sync_miner_day_gas': [miner.sync_miner_day_gas, ['POST']],
        '/save_miner_pool': [miner.save_miner_pool, ['POST']],
        '/sync_miner_lotus': [miner.sync_miner_lotus, ['POST']],
        '/sync_overview_day': [overview.sync_overview_day, ['POST']],
        '/sync_overview_stat': [overview.sync_overview_stat, ['POST']],
        '/sync_overview_day_rmd': [overview.sync_overview_day_rmd, ['POST']],
        '/sync_notaries': [datacap.sync_notaries, ['POST']],
        '/sync_plus_client': [datacap.sync_plus_client, ['POST']],
        '/sync_add_verified_client': [datacap.sync_add_verified_client, ['POST']],
        '/sync_deal_provider': [datacap.sync_deal_provider, ['POST']],
        '/sync_datacap_stats': [datacap.sync_datacap_stats, ['POST']],
        '/sync_messages_wallet': [message.sync_messages_wallet, ['POST']],
        '/sync_deal_stat': [deal.sync_deal_stat, ['POST']],
        '/sync_deal_day': [deal.sync_deal_day, ['POST']],
    },
    '/common': {
        '/get_price': [common.get_price, ['POST']],
        '/search': [common.search, ['POST']],
        '/search_miner_or_wallet': [common.search_miner_or_wallet, ['POST']],
        '/search_miner_type': [common.search_miner_type, ['POST']],  # 查询矿工类型,post/worker/owner/miner

    },
    '/stat': {
        '/get_overview_stat': [overview.get_overview_stat, ['POST']],
        '/get_overview_day_list': [overview.get_overview_day_list, ['POST']],
        '/get_gas_trends': [message.get_gas_trends, ['POST']],
        '/get_gas_stat_all': [message.get_gas_stat_all, ['POST']],
        '/get_overview_power_trends': [overview.get_overview_power_trends, ['POST']],
        '/get_overview_stat_list': [overview.get_overview_stat_list, ['POST']],
        '/get_overview_stat_trends': [overview.get_overview_stat_trends, ['POST']],
    },
    '/block_chain': {
        '/get_tipsets': [block.get_tipsets, ['POST']],
        '/get_tipset_info': [block.get_tipset_info, ['POST']],
        '/get_block_info': [block.get_block_info, ['POST']],
        '/get_wallets_list': [wallets.get_wallets_list, ['POST']],
        '/get_wallet_info': [wallets.get_wallet_info, ['POST']],
        '/get_wallet_record': [wallets.get_wallet_record, ['POST']],
        '/get_miners_by_address': [miner.get_miners_by_address, ['POST']],
        '/get_deal_list': [deal.get_deal_list, ['POST']],
        '/get_deal_info_list': [deal.get_deal_info_list, ['POST']],
        '/get_deal_info': [deal.get_deal_info, ['POST']],
        '/get_mpool_list': [message.get_mpool_list, ['POST']],
        '/get_mpool_info': [message.get_mpool_info, ['POST']],
        '/get_message_list': [message.get_message_list, ['POST']],
        '/get_message_method_list': [message.get_message_method_list, ['POST']],
        '/get_message_info': [message.get_message_info, ['POST']],
    },
    '/homepage': {
        '/get_overview': [overview.get_overview, ['POST']],
        '/get_miner_ranking_list_by_power': [miner.get_miner_ranking_list_by_power, ['POST']],
        '/get_miner_ranking_list': [miner.get_miner_ranking_list, ['POST']],
        '/get_miner_pool_ranking_list': [miner.get_miner_pool_ranking_list, ['POST']],
    },
    '/miner': {
        '/get_miner_by_no': [miner.get_miner_by_no, ['POST']],
        '/get_miner_stats_by_no': [miner.get_miner_stats_by_no, ['POST']],
        '/get_miner_gas_stats_by_no': [miner.get_miner_gas_stats_by_no, ['POST']],
        '/get_miner_line_chart_by_no': [miner.get_miner_line_chart_by_no, ['POST']],
        '/get_miner_day_gas_list_by_no': [miner.get_miner_day_gas_list_by_no, ['POST']],
        '/get_miner_blocks': [block.get_blocks, ['POST']],
        '/get_transfer_list': [message.get_transfer_list, ['POST']],
        '/get_transfer_method_list': [message.get_transfer_method_list, ['POST']],
        '/get_miner_health_report_24h_by_no': [miner.get_miner_health_report_24h_by_no, ['POST']],
        '/get_miner_health_report_day_by_no': [miner.get_miner_health_report_day_by_no, ['POST']],
        '/get_wallet_address_estimated_service_day': [miner.get_wallet_address_estimated_service_day, ['POST']],
        '/get_miner_health_report_gas_stat_by_no': [miner.get_miner_health_report_gas_stat_by_no, ['POST']],
        '/get_messages_stat_by_miner_no': [miner.get_messages_stat_by_miner_no, ['POST']],
    },
    '/miner_pool':{
        '/get_miner_pool_by_owner_id': [miner.get_miner_pool_by_owner_id, ['POST']],
        '/get_miner_pool_stats_by_owner_id': [miner.get_miner_pool_stats_by_owner_id, ['POST']],
        '/get_miner_pool_gas_by_owner_id': [miner.get_miner_pool_gas_by_owner_id, ['POST']],
        '/get_miner_pool_line_chart_by_owner_id': [miner.get_miner_pool_line_chart_by_owner_id, ['POST']],
    },
    '/datacap': {
        '/get_notaries_list': [datacap.get_notaries_list, ['POST']],
        '/get_plus_client_list': [datacap.get_plus_client_list, ['POST']],
        '/get_provider_list': [datacap.get_provider_list, ['POST']],
        '/get_datacap_dashboard': [datacap.get_datacap_dashboard, ['POST']],
        '/get_datacap_stats': [datacap.get_datacap_stats, ['POST']],
    },
    '/tool': {
        '/get_is_wallet': [tool.get_is_wallet, ['POST']],
        '/get_wallet_message_list': [tool.get_wallet_message_list, ['POST']],
        '/get_wallet_flow_list': [tool.get_wallet_flow_list, ['POST']],
    },
    '/ecology': {
        '/get_deal_stat': [deal.get_deal_stat, ['POST']],
        '/get_deal_day': [deal.get_deal_day, ['POST']],
    },
    '/inner': {
        '/get_message_list_by_height': [inner_api.get_message_list_by_height, ['POST']],
        '/get_miner_info_by_miner_no': [inner_api.get_miner_info_by_miner_no, ['POST']],
        '/get_wallet_address_change': [inner_api.get_wallet_address_change, ['POST']],  # 1.0使用
        '/get_miner_day_list': [inner_api.get_miner_day_list, ['POST']],
        '/get_init_value': [inner_api.get_init_value, ['POST']],
        '/get_block_count': [inner_api.get_block_count, ['POST']],
        '/get_miner_increment': [inner_api.get_miner_increment, ['POST']],
    }
}

# open_api 专用
url_map_open_api = {
    # '/enterprise/payslip_batch': [open_api.PayslipSync.as_view('openapi_payslip_sync'), ['POST']]

}
