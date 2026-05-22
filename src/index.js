// Register all Azure Functions — import order doesn't matter
require('./functions/ping');
require('./functions/call');
require('./functions/admin/pings');
require('./functions/admin/fanout-endpoints');
require('./functions/admin/config');
require('./functions/admin/rtb-mappings');
require('./functions/admin/stats');
require('./functions/admin/mid-term-storage');
require('./functions/admin/pipeline');
require('./functions/postback');
require('./functions/sms');
require('./functions/sms-batch');
require('./functions/admin/lts-aggregation');
require('./functions/admin/lts-api');
