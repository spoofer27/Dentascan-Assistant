import servicemanager


def main(stop_event):
    try:
        while not stop_event.is_set():
            # do work
            # print("Uploading is working...")
            stop_event.wait(5)  # wait for 5 seconds or until stop_event is set
    except Exception as exc:
        print(f"Error in uploading thread: {exc}")
        servicemanager.LogErrorMsg(f"Error in uploading thread: {exc}")
        raise