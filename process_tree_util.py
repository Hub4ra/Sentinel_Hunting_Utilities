import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import re 
import networkx as nx
from networkx.drawing.nx_pydot import to_pydot
from PIL import Image as PILImage
from IPython.display import Image as DisplayImage 
from IPython.display import HTML
import tempfile
from ast import literal_eval
import io
import random
import hashlib

# Add libraries path to lookup paths
sys.path.append(os.path.abspath('/Workspace/Repos/'))

# This module has the utils for working with the storage and log analytics
import infrastructure
ct = infrastructure.LogAnalyticsClient()


def go_down_the_process_tree(device_id, process_name, process_id, process_creation_time, days_ago, go_down_x_generations, creation_time_window=3):

  ct.set_timespan(datetime.now() - timedelta(days = days_ago), datetime.now())

  query = f"""
  let logs = 
  DeviceProcessEvents
  | project   
              AccountName,
              // For a more accurate join instead of joining on "DeviceName"
              DeviceId = tolower(DeviceId),
              DeviceName = tolower(DeviceName),
              FileName = tolower(FileName),
              InitiatingProcessFileName = tolower(InitiatingProcessFileName),
              ProcessId,
              InitiatingProcessId,
              ProcessCommandLine,
              InitiatingProcessCommandLine,
              ProcessCreationTime,
              InitiatingProcessCreationTime,
              MD5,
              InitiatingProcessMD5,
              InitiatingProcessSignatureStatus = tolower(InitiatingProcessSignatureStatus),
              InitiatingProcessSignerType = tolower(InitiatingProcessSignerType);

  logs
    | where DeviceId =~ "{device_id}" and FileName =~ "{process_name}" and ProcessId == {process_id} and ProcessCreationTime between (todatetime("{process_creation_time}") -{creation_time_window}s .. todatetime("{process_creation_time}") +{creation_time_window}s) // Problems with exact matches on creation time
    | project
        DeviceName,
        DeviceId,
        Gen0_AccountName = AccountName,
        Gen0_Name = FileName,
        Gen0_Id = ProcessId,
        Gen0_CommandLine = ProcessCommandLine,
        Gen0_CreationTime = ProcessCreationTime,
        Gen0_MD5 = MD5

    | join kind=leftouter (
      logs
      | project
          DeviceId,
          Gen1_AccountName = AccountName,
          Gen1_Name = FileName,
          Gen0_Name = InitiatingProcessFileName,
          Gen1_Id = ProcessId,
          Gen0_Id = InitiatingProcessId,
          Gen1_CreationTime = ProcessCreationTime,
          Gen0_CreationTime = InitiatingProcessCreationTime,
          Gen1_CommandLine = ProcessCommandLine,
          Gen1_MD5 = MD5,
          Gen0_MD5 = InitiatingProcessMD5,
          Gen0_SignerType = InitiatingProcessSignerType,
          Gen0_SignatureStatus = InitiatingProcessSignatureStatus
    ) on DeviceId, Gen0_Name, Gen0_Id, Gen0_CreationTime
    | project-away DeviceId1, Gen0_Name1, Gen0_Id1, Gen0_CreationTime1, Gen0_MD51

    CLIMB_DOWN_THE_PROCESS_TREE_PLACEHOLDER
  """

  climbing_string = ""
  for generation in range(1, go_down_x_generations + 1):
    gen_x_children = "Gen" + f"{generation + 1}" + "_"
    gen_x = "Gen" + f"{generation}" + "_"

    climbing_string += f"""
    | join kind=leftouter (
      logs
      | project
          DeviceId,
          {gen_x_children}AccountName = AccountName,
          {gen_x_children}Name = FileName,
          {gen_x}Name = InitiatingProcessFileName,
          {gen_x_children}Id = ProcessId,
          {gen_x}Id = InitiatingProcessId,
          {gen_x_children}Commandline = ProcessCommandLine,
          {gen_x_children}CreationTime = ProcessCreationTime,
          {gen_x}CreationTime = InitiatingProcessCreationTime,
          {gen_x_children}MD5 = MD5,
          {gen_x}MD5 = InitiatingProcessMD5,
          {gen_x}SignerType = InitiatingProcessSignerType,
          {gen_x}SignatureStatus = InitiatingProcessSignatureStatus
    ) on DeviceId, {gen_x}Name, {gen_x}Id, {gen_x}CreationTime
    | project-away DeviceId1, {gen_x}Name1, {gen_x}Id1, {gen_x}CreationTime1, {gen_x}MD51
    """

  query = query.replace("CLIMB_DOWN_THE_PROCESS_TREE_PLACEHOLDER", climbing_string)
  df = ct.kql_pandas_df(query)
  return df


def go_up_the_process_tree(device_id, process_name, process_id, process_creation_time, days_ago=190, go_up_x_generations=10, creation_time_window = 3):

  ct.set_timespan(datetime.now() - timedelta(days = days_ago), datetime.now())

  query = f"""
  let logs = 
  DeviceProcessEvents
  | project   
              AccountName,
              InitiatingProcessAccountName,
              DeviceName = tolower(DeviceName),
              // For a more accurate join
              DeviceId = tolower(DeviceId),
              FileName = tolower(FileName),
              InitiatingProcessFileName = tolower(InitiatingProcessFileName),
              ProcessId,
              InitiatingProcessId,
              ProcessCommandLine,
              InitiatingProcessCommandLine,
              ProcessCreationTime,
              InitiatingProcessCreationTime,
              MD5,
              InitiatingProcessMD5,
              InitiatingProcessSignatureStatus = tolower(InitiatingProcessSignatureStatus),
              InitiatingProcessSignerType = tolower(InitiatingProcessSignerType),
              InitiatingProcessParentFileName,
              InitiatingProcessParentId,
              InitiatingProcessParentCreationTime;
      logs
      | where DeviceId =~ "{device_id}" and FileName =~ "{process_name}" and ProcessId == {process_id} and ProcessCreationTime between (todatetime("{process_creation_time}") -{creation_time_window}s .. todatetime("{process_creation_time}") +{creation_time_window}s) // Problem with exact matches of time
      | project
          DeviceName,
          DeviceId,
          Gen0_AccountName = AccountName,
          Gen0_Name = FileName,
          Gen1_AccountName = InitiatingProcessAccountName,
          Gen1_Name = InitiatingProcessFileName,
          Gen0_Id = ProcessId,
          Gen1_Id = InitiatingProcessId,
          Gen0_Commandline = ProcessCommandLine,
          Gen1_Commandline = InitiatingProcessCommandLine,
          Gen0_MD5 = MD5,
          Gen1_MD5 = InitiatingProcessMD5,
          Gen0_CreationTime = ProcessCreationTime,
          Gen1_CreationTime = InitiatingProcessCreationTime,
          Gen1_SignerType = InitiatingProcessSignerType,
          Gen1_SignatureStatus = InitiatingProcessSignatureStatus,
          Gen1_ParentName = InitiatingProcessParentFileName,
          Gen1_ParentId = InitiatingProcessParentId,
          Gen1_ParentCreationTime = InitiatingProcessParentCreationTime

        CLIMB_UP_THE_TREE_PLACEHOLDER
  """

  climbing_string = ""

  # Build the query for going up the process tree 
  for generation_number in range(1, go_up_x_generations + 1):
    gen_x = "Gen" + f"{generation_number}" + "_"
    next_gen = "Gen" + f"{generation_number + 1}" + "_"
      
    climbing_string += f"""
    | join kind=leftouter ( 

      logs
      | project
          DeviceId,
          {next_gen}AccountName = InitiatingProcessAccountName,
          {gen_x}Name = FileName,
          {next_gen}Name = InitiatingProcessFileName,
          {gen_x}Id = ProcessId,
          {next_gen}Id = InitiatingProcessId,
          {next_gen}Commandline = InitiatingProcessCommandLine,
          {next_gen}MD5 = InitiatingProcessMD5,
          {gen_x}CreationTime = ProcessCreationTime,
          {next_gen}CreationTime = InitiatingProcessCreationTime,
          {next_gen}SignerType = InitiatingProcessSignerType,
          {next_gen}SignatureStatus = InitiatingProcessSignatureStatus,
          {next_gen}ParentName = InitiatingProcessParentFileName,
          {next_gen}ParentId = InitiatingProcessParentId,
          {next_gen}ParentCreationTime = InitiatingProcessParentCreationTime
      )
      on DeviceId, {gen_x}Name, {gen_x}Id, {gen_x}CreationTime
    | project-away DeviceId1, {gen_x}Name1, {gen_x}Id1, {gen_x}CreationTime1
    """

  query = query.replace("CLIMB_UP_THE_TREE_PLACEHOLDER", climbing_string)
  df = ct.kql_pandas_df(query)
  return df


  # Extract the number from each column name
def extract_gen_number(col_name):
    match = re.search(r'Gen(-?\d+)', col_name)
    return int(match.group(1)) if match else 99999


def get_color(sig_status, last=False):
    if last:
        return "yellow"
    elif pd.isna(sig_status):  # Check if sig_status is NA
        return "gray"
    elif sig_status == "valid":
        return "green"
    elif sig_status == "unsigned" or sig_status == "invalid":
        return "orange"
    else:
        return "gray"


def random_contrasting_color():
    while True:
        r = random.randint(0, 255)
        g = random.randint(0, 255)
        b = random.randint(0, 255)

        # Calculate brightness using luminance formula
        brightness = (0.299 * r + 0.587 * g + 0.114 * b)

        if brightness < 180:  # Lower values = darker colors
            return f'#{r:02x}{g:02x}{b:02x}'


def short_hash(s):
    return hashlib.md5(s.encode()).hexdigest()[:6]
  
def get_tree_df(device_id, process_name, process_id, process_creation_time, days_ago=190, go_up_x_generations=4, go_down_x_generations = 4, creation_time_window = 3, function_call=False):
    up_the_tree_df = go_up_the_process_tree(device_id=device_id, process_name=process_name, process_id=process_id, process_creation_time=process_creation_time, days_ago=days_ago, go_up_x_generations=go_up_x_generations, creation_time_window=creation_time_window)

    down_the_tree_df = go_down_the_process_tree(device_id=device_id, process_name=process_name, process_id=process_id, process_creation_time=process_creation_time, days_ago=days_ago, go_down_x_generations=go_down_x_generations, creation_time_window=creation_time_window)

    down_the_tree_df.columns = [
        col if "Gen0_" in col else re.sub(r"(Gen)(\d+)", r"\1-\2", col) for col in down_the_tree_df.columns
    ]
    
    if up_the_tree_df.empty and down_the_tree_df.empty:
      print("No log exists for the given process")
      if function_call:
          return pd.DataFrame([{"Nothing": "Nada"}]), DisplayImage(filename="/dbfs/FileStore/tables/pngwing_com.png")
      else:
          return DisplayImage(filename="/dbfs/FileStore/tables/pngwing_com.png")

    # Remove double columns before merging
    if "DeviceName" in up_the_tree_df.columns and "DeviceName" in down_the_tree_df.columns:
      up_the_tree_df.drop(["DeviceName"], axis=1, inplace=True)      
    if "Gen0_AccountName" in up_the_tree_df.columns and "Gen0_AccountName" in down_the_tree_df.columns:
      up_the_tree_df.drop(["Gen0_AccountName"], axis=1, inplace=True)
    if "Gen0_CommandLine" in up_the_tree_df.columns and "Gen0_CommandLine" in down_the_tree_df.columns:
      up_the_tree_df.drop(["Gen0_CommandLine"], axis=1, inplace=True)

    tree_df = up_the_tree_df.merge(down_the_tree_df, on=["DeviceId", "Gen0_Name", "Gen0_Id", "Gen0_CreationTime", "Gen0_MD5"], how="outer")

    # Sort columns by the extracted number, descending
    sorted_cols = sorted(
        tree_df.columns,
        key=extract_gen_number,
        reverse=True
    )

    tree_df = tree_df[sorted_cols]

    if function_call:
      return tree_df, None
    else:
      return tree_df


def visualize_process_tree(device_id, process_name, process_id, process_creation_time, days_ago=190, go_up_x_generations=4, go_down_x_generations=4, creation_time_window=3, file_format="web", show_device=True, graph_layout=None):

  tree_df, potential_pic = get_tree_df(device_id=device_id, process_name=process_name, process_id=process_id, process_creation_time=process_creation_time, days_ago=days_ago, go_up_x_generations=go_up_x_generations, go_down_x_generations=go_down_x_generations, creation_time_window=creation_time_window, function_call=True)

  # Make sure the df isn't empty
  if "Nothing" in tree_df.columns:
    return tree_df, potential_pic

  min_gen = None
  max_gen = None

  for gen in tree_df.columns:
    num = extract_gen_number(gen)

    if min_gen is None or num < min_gen:
      min_gen = num
    if (max_gen is None or num > max_gen) and num != 99999:
      max_gen = num

  # Initialize for later color assignment
  md5_list = []

  for gen in range(min_gen, max_gen + 1):
    md5_list.extend(tree_df[f"Gen{gen}_MD5"].to_list())

  md5_list = list(set(md5_list))
  if '' in md5_list:
    md5_list.remove('')

  # Get the signature status for all hashes in the tree df
  md5_status_query = f"""
  DeviceProcessEvents
  | where InitiatingProcessMD5 in (dynamic({md5_list}))
  | summarize make_set(InitiatingProcessSignatureStatus) by InitiatingProcessMD5
  """

  ct.set_timespan(datetime.now() - timedelta(days = 190), datetime.now())
  md5_status_df = ct.kql_pandas_df(md5_status_query)

  hash_to_color_dict = {}

  invalid_statuses = ["Invalid", "Unsigned"]
  valid_status = ["Valid"]

  # Create the hash_to_color dictionary from the logs
  for i, row in md5_status_df.iterrows():
    unique_status_list = literal_eval(row["set_InitiatingProcessSignatureStatus"])
    md5 = row["InitiatingProcessMD5"]
    # More than one status for the hash
    if len(unique_status_list) > 1:
      # If there are conflicting statuses - ignore and let the relevant timestamps in the tree_df determine the color
      if any(status in unique_status_list for status in invalid_statuses) and any(status in unique_status_list for status in valid_status):
        pass

      # If any of them are invalid and not conflicting color orange
      elif any(status in invalid_statuses for status in unique_status_list):
        hash_to_color_dict[md5] = "orange"

      # If any of them are valid and not conflicting color green
      elif any(status in valid_status for status in unique_status_list):
        hash_to_color_dict[md5] = "green"

    # If there is only one item in the list and it is invalid
    elif any(status in invalid_statuses for status in unique_status_list):
      hash_to_color_dict[md5] = "orange"

    # If there is only one item in the list and it is valid
    elif unique_status_list[0] in valid_status:
      hash_to_color_dict[md5] = "green"

    # Every other case
    else:
      hash_to_color_dict[md5] = "gray"

  # Last node will never have a signature status in the logs
  tree_df[f"Gen{min_gen}_SignatureStatus"] = pd.NA

  generation_info = [
    (f"Gen{gen_num}_Name", f"Gen{gen_num}_Id", f"Gen{gen_num}_SignatureStatus", f"Gen{gen_num}_MD5", f"Gen{gen_num}_ParentName", f"Gen{gen_num}_ParentId", f"Gen{gen_num}_CreationTime") for gen_num in range(min_gen, max_gen + 1)
  ]

  # Initialize the directed graph
  G = nx.DiGraph()

  # Initialize the edge color dictionary
  child_hash_and_pid_to_edge_color = {}

  # Get the none-empty row count
  row_count = tree_df.notna().any(axis=1).sum()

  # Determine the color of each process (by hash) before assigning nodes
  for _, row in tree_df.iterrows():
      for i in range(len(generation_info) - 1):

        parent_sig, parent_md5 = row[generation_info[i][2]], row[generation_info[i][3]]
        child_pid, child_sig, child_md5 = row[generation_info[i+1][1]], row[generation_info[i+1][2]], row[generation_info[i+1][3]]

        parent_color = get_color(parent_sig)
        child_color = get_color(child_sig)

        if hash_to_color_dict.get(parent_md5) != "green" and hash_to_color_dict.get(parent_md5) != "orange" and hash_to_color_dict.get(parent_md5) != "gray":
            hash_to_color_dict[parent_md5] = parent_color
        
        if hash_to_color_dict.get(child_md5) != "green" and hash_to_color_dict.get(child_md5) != "orange" and hash_to_color_dict.get(child_md5) != "gray":
            hash_to_color_dict[child_md5] = child_color
        
        # The edge colors only really matters for large graphs
        if row_count > 100:
            if pd.notna(child_md5) and f"{child_md5}{child_pid}" not in child_hash_and_pid_to_edge_color:
                child_hash_and_pid_to_edge_color[f"{child_md5}{child_pid}"] = random_contrasting_color()

  # Avoid trying to color empty logs
  if row_count > 100:
      if '' in child_hash_and_pid_to_edge_color:
        child_hash_and_pid_to_edge_color.pop('')

  # Creating the graph nodes
  for _, row in tree_df.iterrows():
      for i in range(len(generation_info) - 1):

        parent_name, parent_pid, parent_md5, parent_creation_time = row[generation_info[i][0]], row[generation_info[i][1]], row[generation_info[i][3]], row[generation_info[i][6]]
        child_name, child_pid, child_md5, child_creation_time = row[generation_info[i+1][0]], row[generation_info[i+1][1]], row[generation_info[i+1][3]], row[generation_info[i+1][6]]

        if pd.notnull(parent_name) and pd.notnull(child_name) and pd.notnull(parent_pid) and pd.notnull(child_pid):
            
            # Create node labels and node identifiers to handle cases of pid reuse on the same process (multiple parents)
            parent_label = f"{parent_name} ({int(parent_pid)})"
            parent_node_id = f"{parent_label}{short_hash(str(parent_creation_time))}"
            child_label = f"{child_name} ({int(child_pid)})"
            child_node_id = f"{child_label}{short_hash(str(child_creation_time))}"

            parent_color = hash_to_color_dict.get(parent_md5)
            child_color = hash_to_color_dict.get(child_md5)

            # Define node color based on signature status
            G.add_node(parent_node_id, label=parent_label, color=parent_color, style="filled", fillcolor=parent_color)
            G.add_node(child_node_id, label=child_label, color=child_color, style="filled", fillcolor=child_color)

            # Define edge color based on parent process - we choose child md5 for color because we later reverse the order
            edge_color = child_hash_and_pid_to_edge_color.get(f"{child_md5}{child_pid}", "#000000")
            G.add_edge(parent_node_id, child_node_id, color=edge_color)
            

  # This next part is all done to get the last node's parent
  leaf_nodes = [node for node in G.nodes if G.out_degree(node) == 0]

  # Get the node ID
  last_parent_node_id = leaf_nodes[0]
  # Fetch the label attribute from the graph
  last_parent_node_label = G.nodes[last_parent_node_id]['label']
  # Extract Name and pid from label
  last_parent_name = last_parent_node_label.replace("(", "").replace(")", "").split(" ")[0]
  last_parent_id = int(last_parent_node_label.replace("(", "").replace(")", "").split(" ")[1])
  initiating_parent_name = None
  initiating_parent_id = None

  # Get the last parents' initiator
  for gen in range(max_gen + 1):
      name_col = f"Gen{gen}_Name"
      id_col = f"Gen{gen}_Id"

      # Extract last parent initiator
      for _, row in tree_df.iterrows():
          if row[name_col] == last_parent_name and row[id_col] == last_parent_id:
              initiating_parent_name = row[f"Gen{gen}_ParentName"]
              initiating_parent_id = row[f"Gen{gen}_ParentId"]
              break

      if initiating_parent_name:
        break

  # Create the last parent node
  final_node_id = "Hub4ra FTW!!!"
  final_label = f"{initiating_parent_name} ({initiating_parent_id})"
  last_parent_label = f"{last_parent_name} ({last_parent_id})"

  G.add_node(final_node_id, label=final_label, color=get_color(None, last=True), style="filled", fillcolor=get_color(None, last=True))
  G.add_edge(last_parent_node_id, final_node_id)

  if show_device:
      # Add the device node at the top of the tree with no arrow
      device_name = tree_df["DeviceName"][0]
      device_node = f"{device_name} (Device)"
      G.add_node(device_node, color="cyan", fillcolor="cyan")
      G.add_edge(final_node_id, device_node, style="invisible", arrowsize = "0")

  # Use pydot to generate a nice layout
  pydot_graph = to_pydot(G)

  # Change layout based on graph size
  num_nodes = G.number_of_nodes()
  if graph_layout:
    pass
  elif num_nodes > 300:
    graph_layout = "LR"
  else:
    graph_layout = "TB"
    
  # Choose the graph's layout, reverse it because we reverse the arrows
  graph_layout = graph_layout[::-1]
  pydot_graph.set_rankdir(graph_layout)
  
  # Modify the edges to reverse direction
  for edge in pydot_graph.get_edges():
      # Reverse direction for the tree
      edge.set_dir('back')

  pydot_graph.set_graph_defaults(splines='true', nodesep='0.6', ranksep='1.15')

  # Allow the user to change file formats in case of a very large graph
  if num_nodes > 300 and file_format == "web":
    change = input("Your graph is very large and might be of low quality in a web-view. would you like to change it to pdf format?: Y/N")

    if(change.lower()) == "y":
      file_format = "pdf"
    else:
      pass

  if file_format.lower() == "web":
      # Save to a temporary file
      tmp_file = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
      pydot_graph.write_png(tmp_file.name)
      return tree_df, DisplayImage(filename=tmp_file.name)

  # Useful for very large trees - pdf is better suited for it
  if file_format.lower() == "pdf":
      pdf_data = pydot_graph.create(format='pdf')
      # Save the PDF to FileStore so it's web-accessible
      with open("/dbfs/FileStore/output.pdf", "wb") as f:
          f.write(pdf_data)
      return tree_df, HTML('<a href="/files/output.pdf" download target="_blank">Download PDF</a>')