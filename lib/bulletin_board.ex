defmodule User do

  # Subscribes user u to topic t
  # If t does not exist, it is created with u as the only subscriber.
  def subscribe(user_name, topic_name) do
    topic_name = String.to_atom(topic_name)
    master_topic_manager = Process.whereis(topic_name)
    # IO.inspect master_topic_manager
    if (master_topic_manager == nil) do
      # start new topic
      map = Map.new()
      topic = spawn(TopicManager, :run, [topic_name, map, []])
      # register topic in the process registry as master
      Process.register(topic, topic_name)
      # subscribe user to topic
      send(topic, {:subscribe, self(), topic_name, user_name})
    else
      send(master_topic_manager, {:subscribe, self(), topic_name, user_name})
    end
  end

  # unsubscribe user u from topic t
  def unsubscribe(user_name, topic_name) do
    topic_name = String.to_atom(topic_name)
    master_topic_manager = Process.whereis(topic_name)
    send(master_topic_manager, {:unsubscribe, self(), topic_name, user_name})
  end

  # user u posts content regarding topic t
  def post(user_name, topic_name, content) do
    topic_name = String.to_atom(topic_name)
    master_topic_manager = Process.whereis(topic_name)
    send(master_topic_manager, {:post, topic_name, user_name, content})
  end

  def fetch_news() do
    receive do
      {:broadcast, topic_name, content} ->
        IO.puts content
    end
  end

end

defmodule TopicManager do
  def run(topic_name, user_map, secondary_list) do
    receive do
      {:subscribe, user_pid, topic_name, user_name} -> 
        subscribe(user_pid, user_name, topic_name, user_map, secondary_list)
      {:unsubscribe, user_pid, topic_name, user_name} ->
        unsubscribe(user_name, topic_name, user_map, secondary_list)
      {:post, topic_name, user_name, content} ->
        broadcast(topic_name, content, user_map, secondary_list)
      # {:nodedown} ->
      #   exit()
      #   # master node is down, initiate take over protocol
    end
  end

  def subscribe(user_pid, user_name, topic_name, user_map, secondary_list) do
    user_map = Map.put(user_map, user_name, user_pid)
    run(topic_name, user_map, secondary_list)

    # # get all registered topic names
    # topics = Process.registered()

    # if Enum.member?(topics, topic_name) do
    #   # topic already exists
    #   # send user_name to all secondary topic managers
      
    # else
    #   # start new topic
    #   # register topic in the process registry as master
    #   Process.register(self(), topic_name)
      
    #   # spawn secondary topic managers on all nodes
    #   nodes = Node.list()
    #   for node <- nodes do
    #     Node.connect(node)
    #     Node.monitor(node, true)
    #     Node.spawn(node, :run, [topic_name, [user_name]])
    #   end
    #   run(topic_name, [user_name])
    # end
  end

  def unsubscribe(user_name, topic_name, user_map, secondary_list) do
    user_map = Map.delete(user_map, user_name)
    run(topic_name, user_map, secondary_list)
  end

  def broadcast(topic_name, content, user_map, secondary_list) do
    IO.inspect user_map
    # get pids for all users
    pids = Map.values(user_map)
    # Send content to all users 
    for pid <- pids do
      send(pid, {:broadcast, topic_name, content})
    end
    run(topic_name, user_map, secondary_list)
  end

end

User.subscribe("Alice", "computing")
User.subscribe("Bob", "computing")
User.post("Bob", "computing", "Bob here")
# One for Alice and Bob
User.fetch_news()
User.fetch_news()
User.unsubscribe("Bob", "computing")
User.unsubscribe("Alice", "computing")
