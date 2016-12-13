# External:
import os

        for id in range(2):
            # Re-assign the real stdout/stderr back to (1) and (2)
            os.dup2(self.save_fds[id],id+1)
        return

    def close(self):
        for id in range(2):
            # Close the null files
            os.close(self.null_fds[id])
        #Close the duplicates:
        #Very important otherwise "too many files open"
        map(os.close,self.save_fds)
        return

class dummy_semaphore:
    def acquire(self):
        return 
    def release(self):
        return

