from django.test import TestCase
from restaurant.models import MenuItem

class MenuItemsViews(TestCase):
    def setUp(self):
        shakshuka = MenuItem.objects.create(dish='shakshuka', price=8.50, inventory=10)
        Hummus = MenuItem.objects.create(dish='Hummus', price=5.00, inventory=20)
    
    def test_menu_items_list(self):
        response = self.client.get('/menu/')
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'shakshuka')
        self.assertContains(response, 'Hummus')