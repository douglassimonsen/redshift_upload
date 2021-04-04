from graphviz import Digraph
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
dot = Digraph(comment="Code Flow")

dot.node('A', 'Check option coherence', style='filled', fillcolor='#add8e6')
dot.node('B', 'Start default logging', style='filled', fillcolor='#add8e6')
dot.node('C', 'Load data from source', style='filled', fillcolor='#add8e6')
dot.node('D', '<Get predefined columns from DB<br/>[If <b>skip_checks</b> is False]>')
dot.node('E', 'Coerce data to proper types')
dot.node('F', 'Check coherence with table in DB (no extra local columns)')
dot.node('G', '<Save dependent views locally<br/>[If <b>skip_views</b> is False]>')
dot.node('H', 'Break data into chunks', style='filled', fillcolor='#add8e6')
dot.node('I', 'Load data into intermediate data store (S3)', style='filled', fillcolor='#add8e6')
dot.node('J', 'Copy data from data store to DB', style='filled', fillcolor='#add8e6')
dot.node('K', '<Recreate views from local data<br/>[If <b>skip_views</b> is False]>')
dot.node('L', '<Record upload session<br/>[If <b>records_table</b> is not None]>')
dot.node('M', '<Clean up S3 files<br/>[If <b>cleanup_s3</b> is True]>')
dot.node('N', '<Return interface object for reuse<br/>[If <b>close_on_end</b> is False]>')

dot.edge('A', 'B')
dot.edge('B', 'C')

dot.edge('C', 'D', style="dashed")
dot.edge('D', 'E')
dot.edge('E', 'F')

dot.edge('C', 'G', style="dashed")

dot.edge('C', 'H')

dot.edge('H', 'I')
dot.edge('I', 'J')

dot.edge('J', 'K', style="dashed")
dot.edge('J', 'L', style="dashed")
dot.edge('J', 'M', style="dashed")
dot.edge('J', 'N', style="dashed")

dot.node('legend', 'The flow of the upload process. Nodes in light blue are the core components that run every time.   \nOther nodes can be activated by changing the upload_options dictionary. Options are run left   \nto right (that is, "Recreate views from local data" will run before "Clean up S3 files".   ', shape='rectangle', lp='0,0!')

dot.render('process_flow')
