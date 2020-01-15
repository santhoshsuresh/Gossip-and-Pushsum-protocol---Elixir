defmodule Topology do

  def organiseNodes(algorithm, topo_name, numNodes) do
    table = :ets.new(:table, [:named_table,:public])
    case algorithm do
      "gossip" -> :ets.insert(table,{"Algorithm","gossip"})
      "push-sum" -> :ets.insert(table,{"Algorithm","push-sum"})
    end

    :ets.insert(table,{"Topology",topo_name})
    Topology.buildtopology(topo_name,numNodes)

    :ets.insert(table,{"Disconnected Count",0})
    :ets.insert(table,{"Disconnected Nodes",%{}})
    # :ets.insert(table,{"Disconnected Nodes",[]})
    NodeServer.start_link(0)
  end

  def buildtopology(topo_name, nodes) do
    case topo_name do
      "line" -> buildlinenetwork(nodes)
      "full" -> buildfullnetwork(nodes)
      "rand2d" -> buildrand2Dnetwork(nodes)
      "3dtorus" -> build3dtorusnetwork(nodes)
      "honeycomb" -> buildhoneycombnetwork(nodes, 0)
      "honeycombrand" -> buildhoneycombnetwork(nodes, 1)
      _ -> IO.puts "Invalid topology name"
    end
  end

  def buildlinenetwork(nodes) do
    Enum.each(1..nodes, fn (x) ->
      neighbourlist = cond do
        x == 1 -> [(x+1)]
        x == nodes -> [(x-1)]
        true -> [(x-1),(x+1)]
      end
      NodeServer.update_neighbours(String.to_atom("Child_"<>Integer.to_string(x)), neighbourlist)
    end)
  end

  def buildfullnetwork(nodes) do
    list = Enum.to_list(1..nodes)
    Enum.each(list, fn x ->
      neighbourlist = List.delete_at(list, x-1)
      NodeServer.update_neighbours(String.to_atom("Child_"<>Integer.to_string(x)), neighbourlist)
    end)
  end

  def buildrand2Dnetwork(nodes) do
    map = Map.new()
    factor = trunc(Float.ceil(:math.sqrt(nodes)))
    map =Enum.reduce(1..nodes,map, fn x,acc->
      coordinates = {(Enum.random(0..factor)/factor),(Enum.random(0..factor)/factor)}
      Map.put(acc,x,coordinates)
    end)
    assign2Dneighbours(map)
  end

  def assign2Dneighbours(node_coordinates) do
    Enum.each(Map.keys(node_coordinates), fn x ->
      {rx, ry} = Map.fetch!(node_coordinates, x)
      xmax = cond do
        rx+0.1>1 -> 1
        true -> rx+0.1
      end
      xmin = cond do
        rx-0.1<0 -> 0
        true -> rx-0.1
      end
      ymax = cond do
        ry+0.1>1 -> 1
        true -> ry+0.1
      end
      ymin = cond do
        ry-0.1<0 -> 0
        true -> ry-0.1
      end
      rest_nodes = Map.delete(node_coordinates,x)
      keys = Map.keys(rest_nodes)
      neighbours = Enum.map(keys, fn n ->
        {nx, ny} = Map.fetch!(rest_nodes,n)
        if(nx<=xmax and nx>=xmin and ny<=ymax and ny>=ymin) do
          if(:math.sqrt(:math.pow((rx-nx),2) + :math.pow((ry-ny),2)) < 0.1) do
            # Integer.to_string(n)
            n
          end
        end
      end)
      NodeServer.update_neighbours(String.to_atom("Child_"<>Integer.to_string(x)), Enum.filter(neighbours, & &1))
    end)
  end

  def build3dtorusnetwork(nodes) do
    side = trunc(Float.ceil(sidelength(3,nodes))) - 1
    coord = setposition3dtorus(0, 0, 0, side, nodes)
    keys = Map.keys(coord)
    Enum.each(keys, fn c ->
      {x,y,z} = c
      xmin = if x-1<0, do: side, else: x-1
      xmax = if x+1>side, do: 0, else: x+1
      ymin = if y-1<0, do: side, else: y-1
      ymax = if y+1>side, do: 0, else: y+1
      zmin = if z-1<0, do: side, else: z-1
      zmax = if z+1>side, do: 0, else: z+1
      neighbour_pos = %{{xmin,y,z} => "xmin", {xmax,y,z} => "xmax", {x,ymin,z} => "ymin", {x,ymax,z} => "ymax", {x,y,zmin} => "zmin", {x,y,zmax} => "zmax"}
      active_neighbours = Enum.map(Map.keys(neighbour_pos), fn x ->
        if(Map.fetch(coord, x) == :error) do
          pos = Map.fetch!(neighbour_pos, x)
          find3dneighbour(coord, x, pos, side)
        else
          x
        end
      end)
      neighbours = Enum.map(active_neighbours, fn co -> Map.fetch!(coord, co) end)
      node_no = String.to_atom("Child_"<>Integer.to_string(Map.fetch!(coord, c)))
      NodeServer.update_neighbours(node_no, neighbours)
    end)
  end

  def find3dneighbour(coordinate_list, cur_pos, pos, side) do
    {cx, cy, cz} = cur_pos
    case pos do
      "xmin" ->
        if(Map.fetch(coordinate_list, {cx-1, cy, cz}) == :error) do
          find3dneighbour(coordinate_list, {cx-1, cy, cz}, pos, side)
        else
          {cx-1, cy, cz}
        end
      "xmax" -> {0, cy, cz}
      "ymin" ->
        if(Map.fetch(coordinate_list, {cx, cy-1, cz}) == :error) do
          find3dneighbour(coordinate_list, {cx, cy-1, cz}, pos, side)
        else
          {cx, cy-1, cz}
        end
      "ymax" -> {cx,0,cz}
      "zmin" ->
        if(Map.fetch(coordinate_list, {cx, cy, cz-1}) == :error) do
          find3dneighbour(coordinate_list, {cx, cy, cz-1}, pos, side)
        else
          {cx, cy, cz-1}
        end
      "zmax" -> {cx,cy,0}
    end
  end

  def setposition3dtorus(x, y, z, side, nodes, list \\ [], count \\ 0)
  def setposition3dtorus(_x, _y, _z, _side, nodes, list, count) when count>=nodes do
    map = Map.new()
    Enum.reduce(0..nodes-1,map, fn x,acc->
      Map.put(acc,Enum.at(list, x), x+1)
    end)
  end

  def setposition3dtorus(x, y, z, side, nodes, list, count) when count<nodes do
    cond do
      x==side and y==side and z==side ->
        # IO.inspect "0 -> #{x} #{y} #{z} count -> #{count} node -> #{nodes}"
        setposition3dtorus(x,y,z, side, nodes, list ++ [{x,y,z}], count+1)

      y==side and z==side ->
        # IO.inspect "1 -> #{x} #{y} #{z} count -> #{count} node -> #{nodes}"
        setposition3dtorus(x+1,0,0, side, nodes, list ++ [{x,y,z}], count+1)

      z==side ->
        # IO.inspect "2 -> #{x} #{y} #{z} count -> #{count} node -> #{nodes}"
        setposition3dtorus(x,y+1,0, side, nodes, list ++ [{x,y,z}], count+1)

      z<side ->
        # IO.inspect "3 -> #{x} #{y} #{z} count -> #{count} node -> #{nodes}"
        setposition3dtorus(x,y,z+1, side, nodes, list ++ [{x,y,z}], count+1)
    end
  end

  def sidelength(n, x, precision \\ 1.0e-5) do
    f = fn(prev) -> ((n - 1) * prev + x / :math.pow(prev, (n-1))) / n end
    fixed_point(f, x, precision, f.(x))
  end

  defp fixed_point(_, guess, tolerance, next) when abs(guess - next) < tolerance, do: next
  defp fixed_point(f, _, tolerance, next), do: fixed_point(f, next, tolerance, f.(next))

  def buildhoneycombnetwork(nodes,rand) do
    map = constructhoneycomb(0 , 0, nodes, 0)
    keys = Map.keys(map)
    Enum.each(keys, fn c ->
      {x,y} = c
      neighbours = (
      if(rem(y,2) == 0) do #uptree
        [{x,y-1}, {x-1,y+1}, {x+1,y+1}]
      else
        [{x,y+1}, {x-1,y-1}, {x+1,y-1}]
      end)
      active_neighbours = Enum.map(neighbours, fn n ->
        if(Map.fetch(map, n) != :error) do
          Map.fetch!(map, n)
        end
      end)
      node_no = String.to_atom("Child_"<>Integer.to_string(Map.fetch!(map, c)))
      if rand==1 do
        active_neighbours = active_neighbours ++ [Enum.random(1..nodes)]
        NodeServer.update_neighbours(node_no, Enum.filter(active_neighbours, & &1))
        # IO.inspect Enum.filter(active_neighbours, & &1)
      else
        NodeServer.update_neighbours(node_no, Enum.filter(active_neighbours, & &1))
      end
    end)
  end

  def constructhoneycomb(x, y, nodes, row, list \\[], count \\0)
  def constructhoneycomb(_x, _y, nodes, _row, list, count) when count==nodes do
    map = Map.new()
    Enum.reduce(0..nodes-1,map, fn x,acc->
      Map.put(acc,Enum.at(list, x), x+1)
    end)
  end

  def constructhoneycomb(x, y, nodes, row, list, count) when count<nodes and row==0 do
    if y == 10 do
      constructhoneycomb(x+1, 0, nodes, 0, list++[{x,y}], count+1)
    else
      constructhoneycomb(x, y+3, nodes, 1, list++[{x,y}], count+1)
    end
  end

  def constructhoneycomb(x, y, nodes, row, list, count) when count<nodes and row==1 do
    if y == 11 do
      constructhoneycomb(x+1, 1, nodes, 1, list++[{x,y}], count+1)
    else
      constructhoneycomb(x, y+1, nodes, 0, list++[{x,y}], count+1)
    end
  end

end
