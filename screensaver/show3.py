import pygame
pygame.init()
screen = pygame.display.set_mode(1024,768) # Whatever resolution you want.
image  = pygame.image.load('filname.jpg')
screen.blit(image, (0,0)) 
pygame.display.flip()
