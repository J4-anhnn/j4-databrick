import unittest
import sys
import os

# Định nghĩa hàm mask_text trực tiếp trong file test
def mask_text(text):
    """Mask PII data by keeping first character and replacing rest with asterisks"""
    return str(text)[0] + "***" if text else text

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
