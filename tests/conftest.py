import sys
from pathlib import Path

main_folder = Path(__file__).parent.parent
sys.path.insert(0, str(main_folder))
sys.path.insert(0, str(main_folder / 'inspector'))
sys.path.insert(0, str(main_folder / 'tests'))
