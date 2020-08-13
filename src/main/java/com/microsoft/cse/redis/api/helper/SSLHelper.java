package com.microsoft.cse.redis.api.helper;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public class SSLHelper
{

	public static SSLContext getSslContext(String keystoreFile, String password, String protocol, String provider)
			throws GeneralSecurityException, IOException
	{
		KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
		try (InputStream in = new FileInputStream(keystoreFile))
		{
			keystore.load(in, password.toCharArray());
		}
		KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keyManagerFactory.init(keystore, password.toCharArray());

		TrustManagerFactory trustManagerFactory = TrustManagerFactory
				.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(keystore);

		SSLContext sslContext = SSLContext.getInstance(protocol, provider);
		sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());

		return sslContext;
	}

	public static SSLSocketFactory createSslSocketFactory() throws Exception
	{

		String ks = ConfigurationManager.getKeyStoreFileLocation();
		String kspassword = ConfigurationManager.getKeyStorePassword();
		String protocol = ConfigurationManager.getSecurityProtocol();
		String provider = ConfigurationManager.getSecurityProvider();

		SSLContext sslContext = getSslContext(ks, kspassword, protocol, provider);
		return sslContext.getSocketFactory();
	}

}
