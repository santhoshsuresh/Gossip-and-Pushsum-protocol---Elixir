defmodule NodeServer do
  use GenServer

  #********************************************** Client **********************************************#
  def start_link(node) do
    GenServer.start_link(__MODULE__, node, name: String.to_atom("Child_"<>Integer.to_string(node)))
  end

  def update_neighbours(node_id, neighbourlist) do
    GenServer.cast(node_id, {:updateneighbour, neighbourlist})
  end

  def getneightbours(node_id) do
    GenServer.call(node_id, {:get_neigbhours})
  end

  def getnodestatus(node) do
    GenServer.call(node, {:getstatus},:infinity)
  end

  #*********************************** Gossip <start> ******************************************#

  # Function that initiates Gossip
  def initiateGossip(numNodes, startTime) do
    String.to_atom("Child_"<>Integer.to_string(:rand.uniform(numNodes))) |> GenServer.cast({:receive, numNodes, startTime})
    NodeServer.waitfunc(startTime)
  end

  def waitfunc(startTime) do
    endTime = System.os_time(:millisecond)
    time = endTime - startTime
    if(time<=1200000) do waitfunc(startTime)
    else
      IO.puts "Convergence could not be reached within 1200000ms"
      [{_, discNodeCount}] = :ets.lookup(:table,"Disconnected Count")
      IO.puts "No of disconnected Nodes : #{discNodeCount}"
      System.halt(1)
    end
  end

  def disconnectNode(nodeId, numNodes, startTime) do
    # fetch all offline nodes
    [{_, offlineNodes}] = :ets.lookup(:table,"Disconnected Nodes")
    if Map.has_key?(offlineNodes,nodeId) == :false do
      :ets.insert(:table,{"Disconnected Nodes", Map.put(offlineNodes, nodeId, nodeId)})
      :ets.update_counter(:table,"Disconnected Count",{2,1})
    end
    # check for convergence
    [{_,dcount}]=:ets.lookup(:table,"Disconnected Count")
    if(dcount >= trunc(numNodes*0.9)) do
      GenServer.call(:Child_0,{:converge,startTime})
    end
  end

  def getRandAliveNeighbour(neighList) do
    if Enum.empty?(neighList) do
      :false
    else
      rand = Enum.random(neighList)
      nodeName = NodeServer.convToAtom(rand)
      [{_, offlineNodes}] = :ets.lookup(:table,"Disconnected Nodes")
      # IO.inspect offlineNodes
      if Map.has_key?(offlineNodes, Process.whereis(nodeName)) do
        getRandAliveNeighbour(List.delete(neighList,rand))
      else
        NodeServer.convToAtom(rand)
      end
    end
  end

  def convToAtom(num) do
    String.to_atom("Child_"<>Integer.to_string(num))
  end
  #*********************************** Gossip <end> ******************************************#

  #********************************************** Server **********************************************#
  def init(node) do
    # IO.puts "Child #{node} has been started"
    {:ok, %{"msgcount" => 0, "neighbours" => [], "sum" => node, "weight" => 1, "status" => :oblivious}}
  end

  def handle_cast({:updateneighbour, list}, state) do
    {:noreply, Map.put(state, "neighbours", list)}
  end

  def handle_call({:get_neigbhours}, _from, state) do
    {:reply, state["neighbours"], state}
  end

  def handle_call({:getstatus},_from,status) do
    {:reply, status, status}
  end


  #*********************************** Gossip <start> ******************************************#
  def handle_call({:converge, startTime}, _from, _status) do
    endTime = System.os_time(:millisecond)
    time = endTime-startTime
    IO.puts("Convergence reached at : #{inspect time}ms")
    IO.puts "*****************************************************************************"
    System.halt(1)
  end

  def handle_cast({:receive, numNodes, startTime}, node_status) do
    if node_status["msgcount"] >= 10 do
      # call function to disconnect node
      node_status=Map.put(node_status, "status", :disconnected)
      disconnectNode(self(),numNodes, startTime)
    else
      if node_status["msgcount"] == 0 do
        node_status=Map.put(node_status, "status", :infected)
        GenServer.cast(self(), {:send_message, numNodes, startTime})
      else
        GenServer.cast(self(), {:send_message, numNodes, startTime})
      end
    end
    node_status=Map.put(node_status, "msgcount", node_status["msgcount"] + 1)
    {:noreply, node_status}
  end

  def handle_cast({:send_message, numNodes, startTime}, node_status) do
    # retrieve a random neighbour
    destNode = getRandAliveNeighbour(node_status["neighbours"])
    if (destNode != :false) do
      spawn_link( fn -> GenServer.cast(destNode, {:receive, numNodes, startTime}) end)
      GenServer.cast(self(), {:send_message, numNodes, startTime})
    end
    {:noreply,node_status}
  end


  #*********************************** Gossip <end> ******************************************#

  #*********************************** Push-Sum <start> ******************************************#
  def handle_cast({:updateNodeStatus,nodeStatus},state) do
    newstate=Map.put(state, "status", nodeStatus)
    {:noreply,newstate}
  end

  def handle_cast({:updateNodePushSum,nsum,nweight,nmsgcount},node_status) do
    node_status=Map.put(node_status, "sum", nsum)
    node_status=Map.put(node_status, "weight", nweight)
    node_status=Map.put(node_status, "msgcount", nmsgcount)
    {:noreply, node_status}
  end

  def handle_call({:terminate,_numNodes,startTime,dcount,node_status},_from,_state) do
    endTime = System.os_time(:millisecond)
    time = endTime-startTime
    IO.puts("Convergence reached at : #{inspect time}ms")
    IO.puts("Nodes converged: #{dcount}")
    IO.puts("Sum : #{node_status["sum"]}")
    IO.puts("Weight : #{node_status["weight"]}")
    IO.puts("Convergence ratio S/W : #{(node_status["sum"]/node_status["weight"])}")
    IO.puts "*****************************************************************************"
    System.halt(1)
  end

  def startPushSum(numNodes, startTime) do
    IO.puts "Push-sum started for #{numNodes} nodes"
    Enum.map(1..numNodes, fn x ->
      String.to_atom("Child_"<>Integer.to_string(x)) |> propagatePushSum(0,0,numNodes,startTime)
    end)
  end

  def propagatePushSum(nodeName,  sum,  weight, numNodes, startTime) do
    node_status = NodeServer.getnodestatus(nodeName)
    msgcount = node_status["msgcount"]
    nodestate = node_status["status"]
    oldSum = node_status["sum"]
    oldWeight = node_status["weight"]

    newSum = oldSum + sum
    newWeight = oldWeight + weight

    if (newWeight !=0 and oldWeight !=0) do

      diff = abs((newSum/newWeight)-(oldSum/oldWeight))

      if (nodestate != :disconnected) do
        newmsgcount = calculateCounter(nodeName, numNodes, startTime, msgcount, diff, node_status)
        GenServer.cast(nodeName, {:updateNodePushSum,newSum/2,newWeight/2,newmsgcount})

        neighbourNode = getNeighboursPushSum(node_status["neighbours"])

        if (neighbourNode != :false) do
          val1 = if (msgcount != -1) do
            newSum/2
          else
            oldSum/2
          end
          val2 = if (msgcount != -1) do
            newWeight/2
          else
            oldWeight/2
          end
          spawn_link(fn -> propagatePushSum(neighbourNode, val1, val2, numNodes, startTime) end)
          Process.sleep(100)
        else
          stopPushSumExecution(nodeName, numNodes, startTime, node_status)
        end
      end
    else
      NodeServer.updateNodeStatus(nodeName, :disconnected)
      stopPushSumExecution(nodeName, numNodes, startTime, node_status)
    end
  end

  def calculateCounter(nodeName,numNodes,startTime,msgcount,diff,node_status) do
    nodestate = node_status["status"]
    if (msgcount == 2) do
      NodeServer.updateNodeStatus(nodeName, :disconnected)
      stopPushSumExecution(nodeName, numNodes, startTime, node_status)
      -1
    else
      if (msgcount==0) do
        if (nodestate == :oblivious) do
          NodeServer.updateNodeStatus(nodeName, :infected)
          0
        else
          #count 0 and state is infected
          if (diff >= :math.pow(10, -10)) do
            0
          else
            1
          end
        end
      else
        #msgcount is 1 - state would be infected
        if (diff >= :math.pow(10, -10)) do
          0
        else
          msgcount+1
        end
      end
    end
  end

  def getNeighboursPushSum(neighList) do
    if(Enum.empty?(neighList)) do
      :false
    else
      rand = Enum.random(neighList)
      nodeName = NodeServer.convToAtom(rand)
      node_status = NodeServer.getnodestatus(nodeName)
      if (node_status["status"] == :disconnected) do
        getNeighboursPushSum(List.delete(neighList,rand))
      else
        nodeName
      end
    end
  end

  def stopPushSumExecution(nodeName, numNodes, startTime, node_status) do
    [{_, offlineNodes}] = :ets.lookup(:table,"Disconnected Nodes")
    if Map.has_key?(offlineNodes,nodeName) == :false do
      :ets.insert(:table,{"Disconnected Nodes", Map.put(offlineNodes, nodeName, nodeName)})
      :ets.update_counter(:table,"Disconnected Count",{2,1})
    end
    [{_,dcount}]=:ets.lookup(:table,"Disconnected Count")
    [{_,_algo}]=:ets.lookup(:table,"Algorithm")
    # IO.inspect "Disconnecting Node #{nodeName}"<>", Disconnected count: #{dcount}"

    percent = if _algo = "push-sum" do
      [{_,topology}]=:ets.lookup(:table,"Topology")
      case topology do
        "line" -> 0.9
        "3dtorus" -> 0.95
        "rand2D" -> 0.75
        "honeycombrand" -> 0.60
        _ -> 0.85
      end
    else
      0.9
    end

    if (dcount >= trunc(numNodes*percent)) do
      GenServer.call(:Child_0,{:terminate,numNodes,startTime,dcount,node_status})
    end

  end

  def updateNodeStatus(nodeName, nodeStatus) do
    GenServer.cast(nodeName, {:updateNodeStatus,nodeStatus})
  end

  #*********************************** Push-Sum <end> ******************************************#
end
