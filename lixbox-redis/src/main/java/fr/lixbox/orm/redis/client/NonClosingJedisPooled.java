package fr.lixbox.orm.redis.client;

import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisPooled;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import java.net.URI;

class NonClosingJedisPooled extends JedisPooled {

	NonClosingJedisPooled(GenericObjectPoolConfig<Connection> poolConfig, URI uri, int connectionTimeout, int soTimeout) {
		super(poolConfig, uri, connectionTimeout, soTimeout);
	}

	@Override
	public void close() {
		// No-op : instance partagée, fermée uniquement par le owner via reallyClose()
	}

	void reallyClose() {
		super.close();
	}
}