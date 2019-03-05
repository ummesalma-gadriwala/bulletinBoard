defmodule User do

  def start(user_name) do
    # spawn new thread for user
    pid = spawn(User, :run, [])
    # register user in process registry
    user_name = String.to_atom(user_name)
    :global.register_name(user_name, pid)
  end

  def run() do
    receive do
      {:subscribe, user_name, topic_name} ->
        master_topic_manager = :global.whereis_name(topic_name)
        send(master_topic_manager, 
            {:subscribe, topic_name, user_name})
      {:unsubscribe, user_name, topic_name} ->
        master_topic_manager = :global.whereis_name(topic_name)
        send(master_topic_manager, 
            {:unsubscribe, topic_name, user_name})
      {:post, user_name, topic_name, content} ->
        master_topic_manager = :global.whereis_name(topic_name)
        send(master_topic_manager, 
            {:post, topic_name, user_name, content})
      {:fetch} ->
        case :erlang.process_info(self(), :message_queue_len) do
          {:message_queue_len, 0} -> IO.puts "No news"
          {:message_queue_len, queue_len} -> fetch_all_news(queue_len)
        end
    end
    run()
  end

  # Subscribes user u to topic t
  # If t does not exist, it is created with u as the only subscriber.
  def subscribe(user_name, topic_name) do
    topic_name = String.to_atom(topic_name)
    user_name = String.to_atom(user_name)
    user_pid = :global.whereis_name(user_name)
    # does topic already exist?
    master_topic_manager = :global.whereis_name(topic_name)
    case master_topic_manager do
      :undefined ->
        # no, start new topic
        topic_pid = spawn(TopicManager, :start, [topic_name, []])
        # register topic in the process registry as master
        :global.register_name(topic_name, topic_pid)
        # subscribe user to topic
        send(user_pid, {:subscribe, user_name, topic_name})
      _ -> 
        # subscribe user to topic
        send(user_pid, {:subscribe, user_name, topic_name})
    end
  end

  # unsubscribe user u from topic t
  def unsubscribe(user_name, topic_name) do
    topic_name = String.to_atom(topic_name)
    user_name = String.to_atom(user_name)
    user_pid = :global.whereis_name(user_name)
    send(user_pid, {:unsubscribe, user_name, topic_name})
  end

  # user u posts content regarding topic t
  def post(user_name, topic_name, content) do
    topic_name = String.to_atom(topic_name)
    user_name = String.to_atom(user_name)
    user_pid = :global.whereis_name(user_name)
    send(user_pid, {:post, user_name, topic_name, content})
  end

  def fetch_news(user_name) do
    user_name = String.to_atom(user_name)
    user_pid = :global.whereis_name(user_name)
    send(user_pid, {:fetch})
  end

  defp fetch_all_news(1) do
    receive do
      {:broadcast, topic_name, content} ->
        IO.puts content
    end
  end
  defp fetch_all_news(queue_len) do
    receive do
      {:broadcast, topic_name, content} ->
        IO.puts content
        fetch_all_news(queue_len-1)
    end
  end

end

defmodule TopicManager do

  def start(topic_name, user_map) do
    # spawn secondary topic managers on all nodes
    nodes = Node.list()

    secondary_topic_manager_list = for node <- nodes do
      Node.monitor(node, true)
      Node.spawn(node, TopicManager, :secondaryStart, [topic_name, user_map, Node.self()])
    end

    for secondary_topic_manager <- secondary_topic_manager_list do
      send(secondary_topic_manager, {:secondarymanager, :update, secondary_topic_manager_list})
    end

    run(topic_name, user_map, secondary_topic_manager_list)
  end

  def secondaryStart(topic_name, user_map, master_node) do
    Node.monitor(master_node, true)
    secondaryRun(topic_name, user_map, [])
  end

  def run(topic_name, user_map, secondary_topic_manager_list) do
    receive do
      {:subscribe, topic_name, user_name} -> 
        subscribe(user_name, topic_name, user_map, secondary_topic_manager_list)

      {:unsubscribe, topic_name, user_name} ->
        unsubscribe(user_name, topic_name, user_map, secondary_topic_manager_list)

      {:post, topic_name, user_name, content} ->
        broadcast(topic_name, content, user_map, secondary_topic_manager_list)

      {:nodedown, node} ->
        master_topic_manager = :global.whereis_name(topic_name)
        # is the master node down?
        case master_topic_manager do
          :undefined -> 
            # master node is down, initiate take over protocol
            # re-register in the process registry as master
            :global.re_register_name(topic_name, self())
            secondary_topic_manager_list = secondary_topic_manager_list -- [Node.self()]
            run(topic_name, user_map, secondary_topic_manager_list)
          _ ->
            # master node is up, remove node from secondary list
            secondary_topic_manager_list = secondary_topic_manager_list -- [node]
            run(topic_name, user_map, secondary_topic_manager_list)
        end
    end
  end

  def secondaryRun(topic_name, user_map, secondary_topic_manager_list) do
    receive do
      {:secondarymanager, :update, secondary_topic_manager_list} -> 
        secondaryRun(topic_name, user_map, secondary_topic_manager_list)

      {:secondarymanager, :subscribe, user_name} -> 
        user_map = user_map ++ [user_name]
        secondaryRun(topic_name, user_map, secondary_topic_manager_list)

      {:secondarymanager, :unsubscribe, user_name} ->
        user_map = user_map -- [user_name]
        secondaryRun(topic_name, user_map, secondary_topic_manager_list)

      {:nodedown, node} ->
        master_topic_manager = :global.whereis_name(topic_name)
        # is the master node down?
        case master_topic_manager do
          :undefined -> 
            # master node is down, initiate take over protocol
            # re-register in the process registry as master
            :global.re_register_name(topic_name, self())
            secondary_topic_manager_list = secondary_topic_manager_list -- [Node.self()]
            run(topic_name, user_map, secondary_topic_manager_list)
          _ ->
            # master node is up, remove node from secondary list
            secondary_topic_manager_list = secondary_topic_manager_list -- [node]
            secondaryRun(topic_name, user_map, secondary_topic_manager_list)
        end
    end
  end

  def subscribe(user_name, topic_name, user_map, secondary_topic_manager_list) do
    # add user to user_map
    user_map = user_map ++ [user_name]
    
    # Subscribe user in all secondary topic managers
    for secondary_topic_manager <- secondary_topic_manager_list do
      send(secondary_topic_manager, {:secondarymanager, :subscribe, user_name})
    end

    run(topic_name, user_map, secondary_topic_manager_list)
  end

  def unsubscribe(user_name, topic_name, user_map, secondary_topic_manager_list) do
    # remove user from user_map
    user_map = user_map -- [user_name]

    # Unsubscribe user in all secondary topic managers
    for secondary_topic_manager <- secondary_topic_manager_list do
      send(secondary_topic_manager, {:secondarymanager, :unsubscribe, user_name})
    end

    run(topic_name, user_map, secondary_topic_manager_list)
  end

  def broadcast(topic_name, content, user_map, secondary_topic_manager_list) do
    # Send content to all users 
    for user <- user_map do
      user_pid = :global.whereis_name(user)
      if (user_pid != :undefined) do
        send(user_pid, {:broadcast, topic_name, content})
      end
    end

    run(topic_name, user_map, secondary_topic_manager_list)
  end
end


# User.start("Alice")
# User.start("Bob")
# User.subscribe("Alice", "computing")
# User.subscribe("Bob", "computing")
# User.post("Bob", "computing", "Bob here")
# User.post("Alice", "computing", "Alice here")
# User.unsubscribe("Alice", "computing")
# User.post("Bob", "computing", "Bob here here")
# User.fetch_news("Alice")
# User.fetch_news("Bob")
# User.unsubscribe("Bob", "computing")
# Node.connect(:beta@UmmeSalmaGadriwala)
# Node.monitor(:beta@UmmeSalmaGadriwala, true)
# Node.monitor(:alpha@UmmeSalmaGadriwala, true)
# :global.registered_names()
# :global.whereis_name(:Alice) 
# :global.whereis_name(:Bob) 
# :global.whereis_name(:computing) 