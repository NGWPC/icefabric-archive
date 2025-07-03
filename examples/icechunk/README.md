# Icechunk Data Viewer
Interface to load, create and view S3 Icechunk-versioned NGWPC datasets

## Install Icechunk Data Viewer:
1. Clone repo: `git clone <repo path>`
2. CD to repo dir: `cd <repo_path>/icechunk_data_viewer`
3. Create virtual environment: `python3.11 -m venv .venv`
4. Start virtual environment: `source .venv/bin/activate`
5. Load dependedencies: `pip install -r requirements.txt`
6. Prepare AWS credentials. Either:
   - [Set up IAM Identity Center and run `aws sso login` prior to notebook launch](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html#sso-configure-profile-token-auto-sso)
   - [Copy/paste credentials into `~/.aws/credentials`](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-format)
7. Start Jupyter notebook: `jupyter notebook --no-browser`
8. Using your browser, navigate to the notebook server instance (i.e. `http://127.0.0.1:8888/tree?token=XXX`). Your specific URL is listed in the terminal output.
9. Open `icechunk_data_ret_and_plot.jpynb` in the GUI.
10. Prior to execution, depending on if you've configured AWS credentials with SSO or copy/pasted them:
    - SSO: Comment out line #11 in block 1 (`os.environ['AWS_PROFILE'] = "XXX_SoftwareEngineersFull"`)
    - Copy/paste: Alter line #11 to overwrite the placeholder string with your profile name found in `~/.aws/credentials`
11. Run the notebook
