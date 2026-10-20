AutoChemy

AutoChemy is a free and user-friendly computational chemistry platform designed to simplify quantum chemistry workflows for students, researchers, and educators. The software provides an integrated graphical environment for preparing, running, and analyzing calculations without requiring extensive command-line experience.

AutoChemy supports both molecular and periodic systems. ORCA and Quantum ESPRESSO are both integrated, enabling workflows for molecular quantum chemistry as well as periodic (solid-state) calculations within a single application.


Installation Guide video : https://www.youtube.com/watch?v=PWoCmkNO5gs

Key Features
Support for molecular and periodic systems
ORCA integration for molecular quantum chemistry (input generation and job setup)
Quantum ESPRESSO integration for periodic systems
xTB-based geometry optimization and semiempirical calculations
Potential Energy Surface (PES) analysis and plotting
Conformational sampling and molecular structure analysis
Orbital localization and orbital visualization tools
Distortion–Interaction Analysis (DIA)
Bond length and molecular geometry analysis
Output parsing and visualization utilities
Local and remote HPC job management support
Interactive molecular viewers and analysis tools
HPC Job Manager

Submit calculations to any HPC cluster, follow their status and fetch results without leaving AutoChemy.

1. Open **Job Manager** in the sidebar and click **Add cluster**. Enter a name, the login-node address, your username, optionally an SSH key, the scheduler (SLURM, PBS, SGE, LSF, or Custom commands) and a remote folder for jobs (default `~/autochemy_jobs`). Use **Test connection** to check the login and that `sbatch`/`qsub` is found; if it isn't, put e.g. `module load slurm` in "Run before commands".
2. In **Input Creator**, set execution mode to HPC, generate the input, and click **🚀 Submit to HPC**. The `.inp` and `.sh` (plus `.gbw`/`.hess`/NEB product files when used) are uploaded into a new sub-folder and submitted. You can also submit any files from **New submission...** in the Job Manager.
3. The Jobs list shows Queued / Running / Completed / Failed / Cancelled and refreshes automatically. For ORCA jobs, a job that has left the queue is marked Completed or Failed by checking the `.out` file.
4. **Browse files** / **View output** show the job folder on the cluster and the end of any file; **Download job folder** copies results to your computer. **Cancel job** and **Resubmit** are also available.

Each user adds their own cluster(s); they are stored in `AutoChemy_User_Data/hpc_profiles.json` on that computer only. Passwords and OTP codes are asked when you connect and are never saved. The first connection asks you to confirm the cluster's host-key fingerprint; a changed key is refused. Requires the `paramiko` package (installed from `requirements.txt`). If your cluster needs a VPN, connect the VPN first.

Purpose

AutoChemy aims to make computational chemistry more accessible by combining commonly used workflows into a single graphical application. With ORCA and Quantum ESPRESSO integrated, it covers both molecular and periodic systems and is particularly useful for teaching, learning, and performing routine quantum chemistry calculations efficiently.

Current Release

Version: Beta v1.1
Platform: Windows

Installation & Setup

To start using AutoChemy:
1. Ensure you have Python 3.10+ installed on your system.
2. Double-click `Start AutoChemy.bat` to install dependencies and launch the application.

> **Note for Windows Users**: Because you downloaded this project from the internet, Windows may display a "Publisher could not be verified" security warning when you first run the `.bat` file. This is completely normal. Simply uncheck the **"Always ask before opening this file"** box and click **Run** to proceed.

License

This project is distributed under the terms of the included license file.

Disclaimer

AutoChemy is developed as an academic and educational project. While every effort is made to ensure scientific accuracy, users should independently verify computational results before using them in research publications.

Feedback and Support

We are researcher-developers and continuously improving AutoChemy. If you encounter bugs, scientific inconsistencies, or feature requests, please report them through the GitHub repository.

If you find AutoChemy useful, please consider supporting the project by sharing it with fellow students, researchers, and computational chemists.
