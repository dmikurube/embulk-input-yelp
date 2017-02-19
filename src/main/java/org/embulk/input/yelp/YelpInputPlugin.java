package org.embulk.input.yelp;

import org.embulk.base.restclient.RestClientInputPluginBase;

public class YelpInputPlugin
        extends RestClientInputPluginBase<YelpInputPluginDelegate.PluginTask>
{
    public YelpInputPlugin()
    {
        super(YelpInputPluginDelegate.PluginTask.class, new YelpInputPluginDelegate());
    }
}
