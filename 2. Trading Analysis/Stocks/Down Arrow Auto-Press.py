'''
pip install rich pyautogui keyboard

🎮 Keyboard Controls

    z → ⏸ Pause

    x → ▶ Resume

    c → 🛑 Stop

'''
import time
import pyautogui
import keyboard
from rich.progress import Progress, BarColumn, TimeRemainingColumn, SpinnerColumn, TextColumn

# ---------------------------------
# CONTROL FLAGS
# ---------------------------------
paused = False
stop_script = False

def pause():
    global paused
    paused = True

def resume():
    global paused
    paused = False

def stop_now():
    global stop_script
    stop_script = True

# ---------------------------------
# HOTKEYS
# ---------------------------------
keyboard.add_hotkey("z", pause)
keyboard.add_hotkey("x", resume)
keyboard.add_hotkey("c", stop_now)

# ---------------------------------
# GIVE TIME TO FOCUS CHROME / TV
# ---------------------------------
time.sleep(10)

total_presses = 510

with Progress(
    SpinnerColumn(),
    "[bold cyan]Down Arrow Auto-Press[/]",
    BarColumn(bar_width=50),
    TextColumn("[bold white]{task.completed}/{task.total}"),
    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
    TimeRemainingColumn(),
) as progress:

    task = progress.add_task("Pressing", total=total_presses)

    for _ in range(total_presses):

        # -------- STOP --------
        if stop_script:
            break

        # -------- PAUSE --------
        while paused:
            time.sleep(0.2)
            if stop_script:
                break

        pyautogui.press("down")
        time.sleep(3)
        progress.update(task, advance=1)

print("\nDone.\n")

# #--------------------------------------------------------------------------------------------------------------
