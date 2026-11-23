"""The catalog must not use stored Statistics orders in place of the funnel."""
import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

class FunnelSourceTests(unittest.TestCase):
    def test_preset_uses_same_funnel_response_as_custom_dates(self):
        source = (ROOT / 'frontend/index.html').read_text()
        function = re.search(r'function wbProdComparison\(p\) \{.*?\n\}', source, re.S).group()
        script = '''const assert = require('node:assert/strict');
let WB_PROD_SALES_PERIOD = '1';
let WB_PROD_CUSTOM = {current:{123:419},previous:{123:350},current_amount:{123:1981351},previous_amount:{123:1625765}};
''' + function + '''
const row = {nm_id:123,sales_comparisons:{'1':{current:500,previous:400,current_amount:2200000}}};
assert.equal(wbProdComparison(row).current,419);
assert.equal(wbProdComparison(row).current_amount,1981351);
WB_PROD_CUSTOM=null;
assert.equal(wbProdComparison(row),null,'Loading/error must not display old orders');
'''
        result = subprocess.run(['node','-e',script],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)

if __name__ == '__main__':
    unittest.main()
