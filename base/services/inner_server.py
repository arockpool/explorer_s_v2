from base.services.agent import Service


def get_app_info(app_id):
    '''
    获取appinfo
    '''
    data = Service.call("SERVER_SYSTEM", "/system/api/get_app_info", data={"app_id": app_id})
    return data
