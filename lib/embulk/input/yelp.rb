Embulk::JavaPlugin.register_input(
  "yelp", "org.embulk.input.yelp.YelpInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
