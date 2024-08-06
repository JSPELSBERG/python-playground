import monkeyimport
import monkeycode 

def patched_fcn(value):
	print("Patched function!")
	print(f"Monkey says: {value*-1}")
	
monkeyimport.say_monkey = patched_fcn

monkeycode.run()
