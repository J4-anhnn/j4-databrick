import unittest
import sys
import os

# Add project root to path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the function to test
from src.pipelines.dlt_pipeline_direct import mask_text

class TestMaskText(unittest.TestCase):
    """Test suite for data privacy masking function"""
    
    def test_basic_masking(self):
        """Test basic masking functionality"""
        self.assertEqual(mask_text("David_King"), "D***")
        self.assertEqual(mask_text("A"), "A***")
    
    def test_edge_cases(self):
        """Test edge cases like None and empty string"""
        self.assertEqual(mask_text(None), None)
        self.assertEqual(mask_text(""), "")
    
    def test_numeric_input(self):
        """Test with numeric input"""
        self.assertEqual(mask_text(123), "1***")
    
    def test_special_characters(self):
        """Test with special characters"""
        self.assertEqual(mask_text("@#$%"), "@***")

if __name__ == "__main__":
    unittest.main()
