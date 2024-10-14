from django.test import TestCase
from restaurant.models import MenuItem

class MenuItemTestCase(TestCase):
    def test_get_item(self):
        menuItem = MenuItem(dish='Burger', price=5.00, inventory=10)
        self.assertEqual(str(menuItem), 'Burger : 5.00')
