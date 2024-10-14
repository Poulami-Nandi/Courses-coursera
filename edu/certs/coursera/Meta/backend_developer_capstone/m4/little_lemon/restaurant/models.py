from django.db import models

# Create your models here.
class Booking(models.Model):
    name = models.CharField(max_length=255)
    no_of_guests = models.IntegerField()
    booking_date = models.DateField()
    
    def __str__(self):
        return self.name

class MenuItem(models.Model):
    dish = models.CharField(max_length=50)
    price = models.DecimalField(max_digits=5, decimal_places=2)
    inventory = models.IntegerField()
    
    def __str__(self):
        return f'{self.dish} : {self.price}'