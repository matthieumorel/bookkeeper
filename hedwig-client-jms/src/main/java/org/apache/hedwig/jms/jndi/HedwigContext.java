package org.apache.hedwig.jms.jndi;

import java.util.Hashtable;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

import org.apache.hedwig.jms.administered.HedwigTopic;

public class HedwigContext implements Context {

    Hashtable<?, ?> env;
    Hashtable<String, String> mappings = new Hashtable<String, String>();

    public HedwigContext(Hashtable<?, ?> env) {
        this.env = env;
        mappings.put("ConnectionFactory", "org.apache.hedwig.jms.administered.HedwigConnectionFactory");
        mappings.put("TopicConnectionFactory", "org.apache.hedwig.jms.administered.HedwigConnectionFactory");
        mappings.put("QueueConnectionFactory", "org.apache.hedwig.jms.administered.HedwigConnectionFactory");

    }

    @Override
    public Object lookup(Name name) throws NamingException {
        return lookup(name.toString());
    }

    @Override
    public Object lookup(String name) throws NamingException {
        if (mappings.containsKey(name)) {
            try {
                return (Class.forName((String) mappings.get(name))).newInstance();
            } catch (InstantiationException e) {
                throw new NamingException("Cannot instantiate [" + name + "] class");
            } catch (IllegalAccessException e) {
                throw new NamingException("Cannot instantiate [" + name + "] class");
            } catch (ClassNotFoundException e) {
                throw new NamingException("Cannot find class [" + name + "]");
            }
        } else {
            if (name.startsWith("topic.")) {
                return new HedwigTopic((String) env.get(name));
            }

            return env.get(name);
        }
    }

    @Override
    public void bind(Name name, Object obj) throws NamingException {
        // TODO Auto-generated method stub

    }

    @Override
    public void bind(String name, Object obj) throws NamingException {
        // TODO Auto-generated method stub

    }

    @Override
    public void rebind(Name name, Object obj) throws NamingException {
        // TODO Auto-generated method stub

    }

    @Override
    public void rebind(String name, Object obj) throws NamingException {
        // TODO Auto-generated method stub

    }

    @Override
    public void unbind(Name name) throws NamingException {
        // TODO Auto-generated method stub

    }

    @Override
    public void unbind(String name) throws NamingException {
        // TODO Auto-generated method stub

    }

    @Override
    public void rename(Name oldName, Name newName) throws NamingException {
        // TODO Auto-generated method stub

    }

    @Override
    public void rename(String oldName, String newName) throws NamingException {
        // TODO Auto-generated method stub

    }

    @Override
    public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void destroySubcontext(Name name) throws NamingException {
        // TODO Auto-generated method stub

    }

    @Override
    public void destroySubcontext(String name) throws NamingException {
        // TODO Auto-generated method stub

    }

    @Override
    public Context createSubcontext(Name name) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Context createSubcontext(String name) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object lookupLink(Name name) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object lookupLink(String name) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NameParser getNameParser(Name name) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NameParser getNameParser(String name) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Name composeName(Name name, Name prefix) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String composeName(String name, String prefix) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object addToEnvironment(String propName, Object propVal) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object removeFromEnvironment(String propName) throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Hashtable<?, ?> getEnvironment() throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() throws NamingException {
        // TODO Auto-generated method stub

    }

    @Override
    public String getNameInNamespace() throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

}
