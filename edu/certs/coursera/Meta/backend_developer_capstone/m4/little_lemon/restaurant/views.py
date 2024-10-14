from django.shortcuts import render
from rest_framework import generics
from rest_framework import viewsets 
from rest_framework import permissions
from .serializers import *
from .models import *

# Create your views here. 
def index(request):
    return render(request, 'index.html')

class MenuItemsView(generics.ListCreateAPIView):
    queryset = MenuItem.objects.all()
    serializer_class = MenuItemSerializer

class MenuItemDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = MenuItem.objects.all()
    serializer_class = MenuItemSerializer
    
class BookingViewSet(viewsets.ModelViewSet):
    permission_classes = [permissions.IsAuthenticated]
    queryset = Booking.objects.all()
    serializer_class = BookingSerializer