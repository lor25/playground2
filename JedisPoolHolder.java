import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

class JedisPoolHolder {

    private static volatile JedisPool instance = null;
    private static String ip;
    private static int port;


    public static void init(String redisIp, int redisPort) {
        ip = redisIp;
        port = redisPort;
    }
    public static JedisPool getInstance() {
        if (instance == null) {
            synchronized (JedisPoolHolder.class) {
                if (instance == null) {
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(100);
                    config.setMinIdle(10);
                    config.setMaxIdle(10);
                    config.setMaxWaitMillis(2000);
                    config.setTestWhileIdle(false);
                    config.setTestOnBorrow(false);
                    config.setTestOnReturn(false);
                    instance = new JedisPool(config, ip, port);
                }
            }
        }
        return instance;
    }

}
