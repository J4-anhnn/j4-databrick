import unittest

from src.pipelines.dlt_pipeline import mask_text

class TestMaskText(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(mask_text("David_King"), "D***")
        self.assertEqual(mask_text("A"), "A***")
        self.assertEqual(mask_text(None), None)
        self.assertEqual(mask_text(""), "")
if __name__ == "__main__":
    unittest.main()
