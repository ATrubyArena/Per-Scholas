first_name = input("What is your first name? ")
last_name = input("What is your last name? ")
print(f"Hello {first_name} {last_name}, Welcome")
favorite_color = input("What is your favorite color? ")
print(f"{first_name} No way! {favorite_color.capitalize()} is mine too!")

valid_response = False
question = input("Do you like going to the beach? ").lower()
while not valid_response:
    
    if question == "yes":
        print("So do I! I love hearing the sound of the waves crashing")
        valid_response = True
    elif question == "no":
        print("Me neither. I dont like how the sand gets everywhere.")
        valid_response = True
    else: 
        print("Sorry, I didn't catch that." )
        question = input("Do you like going to the beach? ").lower()

import random
valid_response = False
while not valid_response:
    roll_dice = input("Type 'Roll' to test your luck!: ").lower()
    if roll_dice == "roll":
        roll = random.randint(1, 20)
        print(f"You rolled a {roll}!")
        valid_response = True
        if roll == 20:
            print("You're a Wizard Harry!")
        elif roll == 1:
            print("Your luck is DOGSHIT just like YOU!")
    else:
        print("Try again.")
