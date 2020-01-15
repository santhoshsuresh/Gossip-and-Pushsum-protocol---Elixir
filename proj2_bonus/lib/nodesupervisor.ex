defmodule NodeSupervisor do
  use Supervisor

  def start_link(numNodes) do
    Supervisor.start_link(__MODULE__, numNodes)
  end

  def init(numNodes) do
    child_nodes = Enum.map(1..numNodes, fn n -> worker(NodeServer, [n], [id: n, restart: :temporary]) end)
    supervise(child_nodes, strategy: :one_for_one)
  end

  def fail_nodes(numNodes, fail_percent) do
    fail_count = trunc((fail_percent/100) * numNodes)
    if(fail_count > 0) do
      Enum.each(Enum.take_random(1..numNodes, fail_count), fn x ->
        nodename = String.to_atom("Child_"<>Integer.to_string(:rand.uniform(numNodes)))
        [{_, offlineNodes}] = :ets.lookup(:table,"Disconnected Nodes")
        :ets.insert(:table,{"Disconnected Nodes", Map.put(offlineNodes, nodename, nodename)})
        NodeServer.updateNodeStatus(nodename, :disconnected)
      end)
    end
  end

end
