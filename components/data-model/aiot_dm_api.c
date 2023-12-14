/**
 * @file aiot_dm_api.c
 * @brief 数据模型模块接口实现文件, 包含了支持物模型数据格式通信的所有接口实现
 * @date 2020-01-20
 *
 * @copyright Copyright (C) 2015-2020 Alibaba Group Holding Limited
 *
 */

#include "dm_private.h"


static int32_t _dm_send_property_post(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg);
static int32_t _dm_send_get_reg_post(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg);
static int32_t _dm_send_event_post(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg);
static int32_t _dm_send_service_reply(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg);
static int32_t _dm_send_property_set_reply(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg);
static int32_t _dm_send_property_batch_post(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg);
static void _dm_recv_register_handler(void *handle, const aiot_mqtt_recv_t *msg, void *userdata);
static void _dm_recv_generic_reply_handler(void *handle, const aiot_mqtt_recv_t *msg, void *userdata);
static void _dm_recv_property_set_handler(void *handle, const aiot_mqtt_recv_t *msg, void *userdata);
static void _dm_recv_async_service_invoke_handler(void *handle, const aiot_mqtt_recv_t *msg, void *userdata);

static const dm_send_topic_map_t g_dm_send_topic_mapping[AIOT_DMMSG_MAX] = {
    {
        "/v1/device/up/getDeviceInfo/%s",
        _dm_send_get_reg_post
    },
    {
        "/v1/device/up/datas/%s",
        _dm_send_property_post
    },
    {
        "/v1/device/up/event/%s",
        _dm_send_event_post
    },
    {
        "/v1/device/up/set_reply/%s",
        _dm_send_property_set_reply
    },
    {
        "/v1/device/up/service_reply/%s",
        _dm_send_service_reply
    },
    {
        "/v1/device/up/datas/%s",
        _dm_send_property_batch_post
    },
};

static const dm_recv_topic_map_t g_dm_recv_topic_mapping[] = {
    {
        "/v1/device/down/registerInfo/%s",
        _dm_recv_register_handler,
    },
    {
        "/v1/device/down/set/%s",
        _dm_recv_property_set_handler,
    },
    {
        "/v1/device/down/service/%s",
        _dm_recv_async_service_invoke_handler,
    },
    {
        "/v1/device/down/event_reply/%s",
        _dm_recv_generic_reply_handler,
    },
};

static void _append_diag_data(dm_handle_t *dm_handle, uint8_t msg_type, int32_t msg_id)
{
    /* append diagnose data */
    uint8_t diag_data[] = { 0x00, 0x30, 0x01, 0x00, 0x00, 0x31, 0x04, 0x00, 0x00, 0x00, 0x00 };
    diag_data[3] = msg_type;
    diag_data[7] = (msg_id >> 24) & 0xFF;
    diag_data[8] = (msg_id >> 16) & 0xFF;
    diag_data[9] = (msg_id >> 8) & 0xFF;
    diag_data[10] = msg_id & 0xFF;
    core_diag(dm_handle->sysdep, STATE_DM_BASE, diag_data, sizeof(diag_data));
}

static int32_t _dm_setup_topic_mapping(void *mqtt_handle, void *dm_handle)
{
    uint32_t i = 0;
    int32_t res = STATE_SUCCESS;
    char *src[1];
    uint8_t src_count = 0;
    char *dn = NULL;
    dm_handle_t *handle = dm_handle;
    if (NULL == (dn = core_mqtt_get_device_name(handle->mqtt_handle))) {
        return STATE_USER_INPUT_MISSING_DEVICE_NAME;
    }

    src[0] = dn;
    src_count = 1;

    for (i = 0; i < sizeof(g_dm_recv_topic_mapping) / sizeof(dm_recv_topic_map_t); i++) {
        aiot_mqtt_topic_map_t topic_mapping;
        core_sprintf(handle->sysdep, &topic_mapping.topic, g_dm_recv_topic_mapping[i].topic, src, src_count,
                        DATA_MODEL_MODULE_NAME);
        topic_mapping.handler = g_dm_recv_topic_mapping[i].func;
        topic_mapping.userdata = dm_handle;

        res = aiot_mqtt_setopt(mqtt_handle, AIOT_MQTTOPT_APPEND_TOPIC_MAP, &topic_mapping);
        if (res < 0) {
            break;
        }
    }
    return res;
}

static int32_t _dm_prepare_send_topic(dm_handle_t *dm_handle, const aiot_dm_msg_t *msg, char **topic)
{
    char *src[4];
    uint8_t src_count = 0;
    char *dn = NULL;

    if (NULL == msg->device_name && NULL == core_mqtt_get_device_name(dm_handle->mqtt_handle)) {
        return STATE_USER_INPUT_MISSING_DEVICE_NAME;
    }

    dn = (msg->device_name != NULL) ? msg->device_name : core_mqtt_get_device_name(dm_handle->mqtt_handle);

    switch (msg->type) {
        case AIOT_DMMSG_PROPERTY_POST:
        case AIOT_DMMSG_PROPERTY_BATCH_POST:
        case AIOT_DMMSG_PROPERTY_SET_REPLY:
        case AIOT_DMMSG_GET_DESIRED:
        case AIOT_DMMSG_DELETE_DESIRED:
        case AIOT_DMMSG_RAW_DATA: {
            src[0] = dn;
            src_count = 1;
        }
        break;
        case AIOT_DMMSG_EVENT_POST: {
            src[0] = dn;
            src_count = 1;
        }
        break;
        case AIOT_DMMSG_ASYNC_SERVICE_REPLY: {
            src[0] = dn;
            src_count = 1;
        }
        break;
        case AIOT_DMMSG_SYNC_SERVICE_REPLY: {
            src[0] = dn;
            src_count = 1;
        }
        break;
        case AIOT_DMMSG_RAW_SERVICE_REPLY: {
            src[0] = dn;
            src_count = 1;
        }
        break;
        default:
            return STATE_USER_INPUT_OUT_RANGE;
    }

    return core_sprintf(dm_handle->sysdep, topic, g_dm_send_topic_mapping[msg->type].topic, src, src_count,
                        DATA_MODEL_MODULE_NAME);
}

static int32_t _dm_send_reg_req(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg)
{
    char *payload = NULL;
    int32_t id = 0;
    char id_string[11] = { 0 };
    char *src[2] = { NULL };
    int32_t res = STATE_SUCCESS;

    if (NULL == msg) {
        return STATE_DM_MSG_PARAMS_IS_NULL;
    }

    core_global_alink_id_next(handle->sysdep, &id);
    core_int2str(id, id_string, NULL);

    _append_diag_data(handle, DM_DIAG_MSG_TYPE_REQ, id);

    src[0] = id_string;
    src[1] = msg->data.get_reg_post.time;


    res = core_sprintf(handle->sysdep, &payload, XJT_GET_DEVICE, src, sizeof(src) / sizeof(char *),
                       DATA_MODEL_MODULE_NAME);
    if (res < 0) {
        return res;
    }

    res = aiot_mqtt_pub(handle->mqtt_handle, (char *)topic, (uint8_t *)payload, strlen(payload), 0);
    handle->sysdep->core_sysdep_free(payload);

    if (STATE_SUCCESS == res) {
        return id;
    }
    return res;
}

static int32_t _dm_send_prop_req(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg)
{
    char *payload = NULL;
    int32_t id = 0;
    char id_string[11] = { 0 };
    char *src[2] = { NULL };
    int32_t res = STATE_SUCCESS;

    if (NULL == msg) {
        return STATE_DM_MSG_PARAMS_IS_NULL;
    }

    core_global_alink_id_next(handle->sysdep, &id);
    core_int2str(id, id_string, NULL);

    _append_diag_data(handle, DM_DIAG_MSG_TYPE_REQ, id);

    src[0] = id_string;
    src[1] = msg->data.property_post.params;


    res = core_sprintf(handle->sysdep, &payload, XJT_PROP_POST, src, sizeof(src) / sizeof(char *),
                       DATA_MODEL_MODULE_NAME);
    if (res < 0) {
        return res;
    }

    res = aiot_mqtt_pub(handle->mqtt_handle, (char *)topic, (uint8_t *)payload, strlen(payload), 0);
    handle->sysdep->core_sysdep_free(payload);

    if (STATE_SUCCESS == res) {
        return id;
    }
    return res;
}
static int32_t _dm_send_event_req(dm_handle_t *handle, const char *topic,const aiot_dm_msg_t *msg)
{
    char *payload = NULL;
    int32_t id = 0;
    char id_string[11] = { 0 };
    char *src[4] = { NULL };
    int32_t res = STATE_SUCCESS;

    if (NULL == msg) {
        return STATE_DM_MSG_PARAMS_IS_NULL;
    }

    core_global_alink_id_next(handle->sysdep, &id);
    core_int2str(id, id_string, NULL);

    _append_diag_data(handle, DM_DIAG_MSG_TYPE_REQ, id);

    src[0] = id_string;
    src[1] = msg->data.xjt_event_post.time;
    src[2] = msg->data.xjt_event_post.event_id;
    src[3] = msg->data.xjt_event_post.params;

    res = core_sprintf(handle->sysdep, &payload, XJT_EVENT_POST, src, sizeof(src) / sizeof(char *),
                       DATA_MODEL_MODULE_NAME);
    if (res < 0) {
        return res;
    }

    res = aiot_mqtt_pub(handle->mqtt_handle, (char *)topic, (uint8_t *)payload, strlen(payload), 0);
    handle->sysdep->core_sysdep_free(payload);

    if (STATE_SUCCESS == res) {
        return id;
    }
    return res;
}
static int32_t _dm_send_service_req(dm_handle_t *handle, const char *topic,const aiot_dm_msg_t *msg)
{
    char *payload = NULL;
    int32_t id = 0;
    char id_string[11] = { 0 };
    char *src[3] = { NULL };
    int32_t res = STATE_SUCCESS;

    if (NULL == msg) {
        return STATE_DM_MSG_PARAMS_IS_NULL;
    }

    core_global_alink_id_next(handle->sysdep, &id);
    core_int2str(id, id_string, NULL);

    _append_diag_data(handle, DM_DIAG_MSG_TYPE_REQ, id);

    src[0] = id_string;
    src[1] = msg->data.xjt_service_rep.code;
    src[2] = msg->data.xjt_service_rep.params;

    res = core_sprintf(handle->sysdep, &payload, XJT_SERVICE_REPLY, src, sizeof(src) / sizeof(char *),
                       DATA_MODEL_MODULE_NAME);
    if (res < 0) {
        return res;
    }

    res = aiot_mqtt_pub(handle->mqtt_handle, (char *)topic, (uint8_t *)payload, strlen(payload), 0);
    handle->sysdep->core_sysdep_free(payload);

    if (STATE_SUCCESS == res) {
        return id;
    }
    return res;
}

static int32_t _dm_send_alink_rsp(dm_handle_t *handle, const char *topic, uint64_t msg_id, uint32_t code,
                                  char *data)
{
    char *payload = NULL;
    char id_string[21] = { 0 };
    char code_string[11] = { 0 };
    char *src[3] = { NULL };
    int32_t res = STATE_SUCCESS;

    if (NULL == data) {
        return STATE_DM_MSG_DATA_IS_NULL;
    }

    core_uint642str(msg_id, id_string, NULL);
    core_uint2str(code, code_string, NULL);

    src[0] = id_string;
    src[1] = code_string;
    src[2] = data;

    res = core_sprintf(handle->sysdep, &payload, ALINK_RESPONSE_FMT, src, sizeof(src) / sizeof(char *),
                       DATA_MODEL_MODULE_NAME);
    if (res < 0) {
        return res;
    }

    res = aiot_mqtt_pub(handle->mqtt_handle, (char *)topic, (uint8_t *)payload, strlen(payload), 0);
    handle->sysdep->core_sysdep_free(payload);

    return res;
}

/*** dm send function start ***/
static int32_t _dm_send_get_reg_post(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg)
{
    return _dm_send_reg_req(handle, topic, msg);
}

static int32_t _dm_send_property_post(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg)
{
    return _dm_send_prop_req(handle, topic, msg);
}

static int32_t _dm_send_event_post(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg)
{
    return _dm_send_event_req(handle, topic, msg);
}
static int32_t _dm_send_service_reply(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg)
{
    return _dm_send_service_req(handle, topic, msg);
}
static int32_t _dm_send_property_set_reply(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg)
{
    return _dm_send_alink_rsp(handle, topic, msg->data.property_set_reply.msg_id,
                              msg->data.property_set_reply.code,
                              msg->data.property_set_reply.data);
}







static int32_t _dm_send_property_batch_post(dm_handle_t *handle, const char *topic, const aiot_dm_msg_t *msg)
{
    return _dm_send_prop_req(handle, topic, msg);
}
/*** dm send function end ***/

/*** dm recv handler functions start ***/
static int32_t _dm_get_topic_level(aiot_sysdep_portfile_t *sysdep, char *topic, uint32_t topic_len, uint8_t level,
                                   char **level_name)
{
    uint32_t i = 0;
    uint16_t level_curr = 0;
    char *p_open = NULL;
    char *p_close = NULL;
    char *p_name = NULL;
    uint16_t name_len = 0;

    for (i = 0; i < (topic_len - 1); i++) {
        if (topic[i] == '/') {
            level_curr++;
            if (level_curr == level && p_open == NULL) {
                p_open = topic + i + 1;
            }

            if (level_curr == (level + 1) && p_close == NULL) {
                p_close = topic + i;
            }
        }
    }

    if (p_open == NULL) {
        return STATE_DM_INTERNAL_TOPIC_ERROR;
    }
    if (p_close == NULL) {
        p_close = topic + topic_len;
    }

    name_len = p_close - p_open;
    p_name = sysdep->core_sysdep_malloc(name_len + 1, DATA_MODEL_MODULE_NAME);
    if (p_name == NULL) {
        return STATE_SYS_DEPEND_MALLOC_FAILED;
    }
    memset(p_name, 0, name_len + 1);
    memcpy(p_name, p_open, name_len);
    *level_name = p_name;

    return STATE_SUCCESS;
}

static int32_t _dm_parse_xjt_prop_request(const char *payload, uint32_t payload_len, uint64_t *msg_id,char **se_id,char ** eid, char **params,
                                       uint32_t *params_len)
{
    char *value = NULL;
    uint32_t value_len = 0;
    int32_t res = STATE_SUCCESS;

    if ((res = core_json_value((char *)payload, payload_len, XJT_JSON_KEY_ID, strlen(XJT_JSON_KEY_ID),
                               &value, &value_len)) < 0 ||
        ((res = core_str2uint64(value, value_len, msg_id)) < 0)) {
        return res;
    }

    if ((res = core_json_value((char *)payload, payload_len, XJT_JSON_KEY_SERVICE_ID, strlen(XJT_JSON_KEY_SERVICE_ID),
                               &value, &value_len)) < 0) {
        return res;
    }
    *se_id = value;

    if ((res = core_json_value((char *)payload, payload_len, XJT_JSON_KEY_EID, strlen(XJT_JSON_KEY_EID),
                               &value, &value_len)) < 0) {
        return res;
    }
    *eid = value;

    if ((res = core_json_value((char *)payload, payload_len, XJT_JSON_KEY_PARAMS, strlen(XJT_JSON_KEY_PARAMS),
                               &value, &value_len)) < 0) {
        return res;
    }

    *params = value;
    *params_len = value_len;

    return res;
}

static int32_t _dm_parse_xjt_service_request(const char *payload, uint32_t payload_len, uint64_t *msg_id,char **ident, char **params,
                                       uint32_t *params_len)
{
    char *value = NULL;
    uint32_t value_len = 0;
    int32_t res = STATE_SUCCESS;

    if ((res = core_json_value((char *)payload, payload_len, XJT_JSON_KEY_ID, strlen(XJT_JSON_KEY_ID),
                               &value, &value_len)) < 0 ||
        ((res = core_str2uint64(value, value_len, msg_id)) < 0)) {
        return res;
    }

    if ((res = core_json_value((char *)payload, payload_len, XJT_JSON_KEY_IDENTIFIER, strlen(XJT_JSON_KEY_IDENTIFIER),
                               &value, &value_len)) < 0) {
        return res;
    }
    *ident = value;

    if ((res = core_json_value((char *)payload, payload_len, XJT_JSON_KEY_DATA, strlen(XJT_JSON_KEY_DATA),
                               &value, &value_len)) < 0) {
        return res;
    }

    *params = value;
    *params_len = value_len;

    return res;
}

static void _dm_recv_register_handler(void *handle, const aiot_mqtt_recv_t *msg, void *userdata)
{
    dm_handle_t *dm_handle = (dm_handle_t *)userdata;
    aiot_dm_recv_t recv;
    char *value = NULL;
    uint32_t value_len = 0;
    int32_t res = STATE_SUCCESS;

    if (NULL == dm_handle->recv_handler) {
        return;
    }

    /* construct recv message */
    memset(&recv, 0, sizeof(aiot_dm_recv_t));
    recv.type = AIOT_DMRECV_REGISTER_INFO;

    core_log(dm_handle->sysdep, STATE_DM_LOG_RECV, "DM recv generic reply\r\n");

    do {
        if (_dm_get_topic_level(dm_handle->sysdep, msg->data.pub.topic, msg->data.pub.topic_len, 5, &recv.device_name) < 0) {
            break;  /* must be malloc failed */
        }

        if ((core_json_value((char *)msg->data.pub.payload, msg->data.pub.payload_len,
                                   XJT_JSON_KEY_ID, strlen(XJT_JSON_KEY_ID), &value, &value_len)) < 0 ||
            (core_str2uint(value, value_len, &recv.data.register_info.msg_id)) < 0) {

            core_log(dm_handle->sysdep, SATAE_DM_LOG_PARSE_RECV_MSG_FAILED, "DM parse generic reply failed\r\n");
            break;
        }

        res = core_json_value((char *)msg->data.pub.payload, msg->data.pub.payload_len,
                        XJT_JSON_KEY_DEV_INFO, strlen(XJT_JSON_KEY_DEV_INFO),
                        &recv.data.register_info.params,
                        &recv.data.register_info.params_len);
        if(res != STATE_SUCCESS) {
            recv.data.register_info.params = NULL;
            recv.data.register_info.params_len = 0;
        }

        _append_diag_data(dm_handle, DM_DIAG_MSG_TYPE_RSP, recv.data.register_info.msg_id);
        dm_handle->recv_handler(dm_handle, &recv, dm_handle->userdata);
    } while (0);

    DM_FREE(recv.device_name);
}

static void _dm_recv_generic_reply_handler(void *handle, const aiot_mqtt_recv_t *msg, void *userdata)
{
    dm_handle_t *dm_handle = (dm_handle_t *)userdata;
    aiot_dm_recv_t recv;
    char *value = NULL;
    uint32_t value_len = 0;
    int32_t res = STATE_SUCCESS;

    if (NULL == dm_handle->recv_handler) {
        return;
    }

    /* construct recv message */
    memset(&recv, 0, sizeof(aiot_dm_recv_t));
    recv.type = AIOT_DMRECV_GENERIC_REPLY;

    core_log(dm_handle->sysdep, STATE_DM_LOG_RECV, "DM recv generic reply\r\n");

    do {
        if (_dm_get_topic_level(dm_handle->sysdep, msg->data.pub.topic, msg->data.pub.topic_len, 2, &recv.product_key) < 0 ||
            _dm_get_topic_level(dm_handle->sysdep, msg->data.pub.topic, msg->data.pub.topic_len, 3, &recv.device_name) < 0) {
            break;  /* must be malloc failed */
        }

        if ((core_json_value((char *)msg->data.pub.payload, msg->data.pub.payload_len,
                                   ALINK_JSON_KEY_ID, strlen(ALINK_JSON_KEY_ID), &value, &value_len)) < 0 ||
            (core_str2uint(value, value_len, &recv.data.generic_reply.msg_id)) < 0 ||
            (core_json_value((char *)msg->data.pub.payload, msg->data.pub.payload_len,
                                   ALINK_JSON_KEY_CODE, strlen(ALINK_JSON_KEY_CODE), &value, &value_len)) < 0 ||
            (core_str2uint(value, value_len, &recv.data.generic_reply.code)) < 0 ||
            (core_json_value((char *)msg->data.pub.payload, msg->data.pub.payload_len,
                                   ALINK_JSON_KEY_DATA, strlen(ALINK_JSON_KEY_DATA),
                                   &recv.data.generic_reply.data,
                                   &recv.data.generic_reply.data_len)) < 0) {

            core_log(dm_handle->sysdep, SATAE_DM_LOG_PARSE_RECV_MSG_FAILED, "DM parse generic reply failed\r\n");
            break;
        }

        res = core_json_value((char *)msg->data.pub.payload, msg->data.pub.payload_len,
                        ALINK_JSON_KEY_MESSAGE, strlen(ALINK_JSON_KEY_MESSAGE),
                        &recv.data.generic_reply.message,
                        &recv.data.generic_reply.message_len);
        if(res != STATE_SUCCESS) {
            recv.data.generic_reply.message = NULL;
            recv.data.generic_reply.message_len = 0;
        }

        _append_diag_data(dm_handle, DM_DIAG_MSG_TYPE_RSP, recv.data.generic_reply.msg_id);
        dm_handle->recv_handler(dm_handle, &recv, dm_handle->userdata);
    } while (0);

    DM_FREE(recv.product_key);
    DM_FREE(recv.device_name);
}

static void _dm_recv_property_set_handler(void *handle, const aiot_mqtt_recv_t *msg, void *userdata)
{
    dm_handle_t *dm_handle = (dm_handle_t *)userdata;
    aiot_dm_recv_t recv;

    if (NULL == dm_handle->recv_handler) {
        return;
    }

    /* construct recv message */
    memset(&recv, 0, sizeof(aiot_dm_recv_t));
    recv.type = AIOT_DMRECV_PROPERTY_SET;

    core_log(dm_handle->sysdep, STATE_DM_LOG_RECV, "DM recv property set\r\n");

    do {
        if (_dm_get_topic_level(dm_handle->sysdep, msg->data.pub.topic, msg->data.pub.topic_len, 5, &recv.device_name) < 0) {
            break;     /* must be malloc failed */
        }

        if ((_dm_parse_xjt_prop_request((char *)msg->data.pub.payload, msg->data.pub.payload_len,
                                           &recv.data.xjt_property.msg_id,
                                           &recv.data.xjt_property.serviceId,
                                           &recv.data.xjt_property.eid,
                                           &recv.data.xjt_property.params,
                                           &recv.data.xjt_property.params_len)) < 0) {

            core_log(dm_handle->sysdep, SATAE_DM_LOG_PARSE_RECV_MSG_FAILED, "DM parse property set failed\r\n");
            break;
        }
        dm_handle->recv_handler(dm_handle, &recv, dm_handle->userdata);
    } while (0);

    DM_FREE(recv.device_name);
}

static void _dm_recv_async_service_invoke_handler(void *handle, const aiot_mqtt_recv_t *msg, void *userdata)
{
    dm_handle_t *dm_handle = (dm_handle_t *)userdata;
    aiot_dm_recv_t recv;

    if (NULL == dm_handle->recv_handler) {
        return;
    }

    memset(&recv, 0, sizeof(aiot_dm_recv_t));
    recv.type = AIOT_DMRECV_ASYNC_SERVICE_INVOKE;

    core_log(dm_handle->sysdep, STATE_DM_LOG_RECV, "DM recv async service invoke\r\n");

    do {
        if (_dm_get_topic_level(dm_handle->sysdep, msg->data.pub.topic, msg->data.pub.topic_len, 5, &recv.device_name) < 0) {
            break;
        }
        if ((_dm_parse_xjt_service_request((char *)msg->data.pub.payload, msg->data.pub.payload_len,
                                           &recv.data.service_down.msg_id,
                                           &recv.data.service_down.identifier,
                                           &recv.data.service_down.params,
                                           &recv.data.service_down.params_len)) < 0) {

            /* core_log(dm_handle->sysdep, SATAE_DM_LOG_PARSE_RECV_MSG_FAILED, "DM parse async servicey failed\r\n"); */
            break;
        }

        dm_handle->recv_handler(dm_handle, &recv, dm_handle->userdata);
    } while (0);

    DM_FREE(recv.device_name);
    DM_FREE(recv.data.async_service_invoke.service_id);
}




static void _dm_core_mqtt_process_handler(void *context, aiot_mqtt_event_t *event, core_mqtt_event_t *core_event)
{
    dm_handle_t *dm_handle = (dm_handle_t *)context;

    if (core_event != NULL) {
        switch (core_event->type) {
            case CORE_MQTTEVT_DEINIT: {
                dm_handle->mqtt_handle = NULL;
                return;
            }
            default: {

            }
            break;
        }
    }
}

static int32_t _dm_core_mqtt_operate_process_handler(dm_handle_t *dm_handle, core_mqtt_option_t option)
{
    core_mqtt_process_data_t process_data;

    memset(&process_data, 0, sizeof(core_mqtt_process_data_t));
    process_data.handler = _dm_core_mqtt_process_handler;
    process_data.context = dm_handle;

    return core_mqtt_setopt(dm_handle->mqtt_handle, option, &process_data);
}

void *aiot_dm_init(void)
{
    aiot_sysdep_portfile_t *sysdep = aiot_sysdep_get_portfile();
    dm_handle_t *dm_handle = NULL;

    if (NULL == sysdep) {
        return NULL;
    }

    dm_handle = sysdep->core_sysdep_malloc(sizeof(dm_handle_t), DATA_MODEL_MODULE_NAME);
    if (NULL == dm_handle) {
        return NULL;
    }

    memset(dm_handle, 0, sizeof(dm_handle_t));
    dm_handle->sysdep = sysdep;
    dm_handle->post_reply = 1;

    core_global_init(sysdep);
    return dm_handle;
}

int32_t aiot_dm_setopt(void *handle, aiot_dm_option_t option, void *data)
{
    dm_handle_t *dm_handle;
    int32_t res = STATE_SUCCESS;

    if (NULL == handle || NULL == data) {
        return STATE_USER_INPUT_NULL_POINTER;
    }
    if (option >= AIOT_DMOPT_MAX) {
        return STATE_USER_INPUT_OUT_RANGE;
    }

    dm_handle = (dm_handle_t *)handle;

    switch (option) {
        case AIOT_DMOPT_MQTT_HANDLE: {
            dm_handle->mqtt_handle = data;
            /* setup mqtt topic mapping */
            res = _dm_setup_topic_mapping(data, dm_handle);
            if (res >= STATE_SUCCESS) {
                res = _dm_core_mqtt_operate_process_handler(dm_handle, CORE_MQTTOPT_APPEND_PROCESS_HANDLER);
            }
        }
        break;
        case AIOT_DMOPT_RECV_HANDLER: {
            dm_handle->recv_handler = (aiot_dm_recv_handler_t)data;
        }
        break;
        case AIOT_DMOPT_USERDATA: {
            dm_handle->userdata = data;
        }
        break;
        case AIOT_DMOPT_POST_REPLY: {
            dm_handle->post_reply = *(uint8_t *)data;
        }
        break;
        default:
            break;
    }

    return res;
}

int32_t aiot_dm_send(void *handle, const aiot_dm_msg_t *msg)
{
    dm_handle_t *dm_handle = NULL;
    char *topic = NULL;
    int32_t res = STATE_SUCCESS;

    if (NULL == handle || NULL == msg) {
        return STATE_USER_INPUT_NULL_POINTER;
    }

    if (msg->type >= AIOT_DMMSG_MAX) {
        return STATE_USER_INPUT_OUT_RANGE;
    }

    dm_handle = (dm_handle_t *)handle;
    if (NULL == dm_handle->mqtt_handle) {
        return STATE_DM_MQTT_HANDLE_IS_NULL;
    }

    res = _dm_prepare_send_topic(dm_handle, msg, &topic);
    if (res < 0) {
        return res;
    }

    res = g_dm_send_topic_mapping[msg->type].func(dm_handle, topic, msg);
    dm_handle->sysdep->core_sysdep_free(topic);
    return res;
}

int32_t aiot_dm_deinit(void **p_handle)
{
    dm_handle_t *dm_handle = NULL;
    aiot_sysdep_portfile_t *sysdep = NULL;
    uint8_t i = 0;

    if (NULL == p_handle || NULL == *p_handle) {
        return STATE_USER_INPUT_NULL_POINTER;
    }

    dm_handle = *p_handle;
    sysdep = dm_handle->sysdep;
    *p_handle = NULL;

    _dm_core_mqtt_operate_process_handler(dm_handle, CORE_MQTTOPT_REMOVE_PROCESS_HANDLER);

    /* remove mqtt topic mapping */
    for (i = 0; i < sizeof(g_dm_recv_topic_mapping) / sizeof(dm_recv_topic_map_t); i++) {
        aiot_mqtt_topic_map_t topic_mapping;
        memset(&topic_mapping, 0, sizeof(aiot_mqtt_topic_map_t));
        topic_mapping.topic = g_dm_recv_topic_mapping[i].topic;
        topic_mapping.handler = g_dm_recv_topic_mapping[i].func;

        aiot_mqtt_setopt(dm_handle->mqtt_handle, AIOT_MQTTOPT_REMOVE_TOPIC_MAP, &topic_mapping);
    }

    sysdep->core_sysdep_free(dm_handle);

    core_global_deinit(sysdep);
    return STATE_SUCCESS;
}

