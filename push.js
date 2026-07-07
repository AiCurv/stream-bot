import { execSync } from "child_process";

const url = "https://" + process.env.GITHUB_TOKEN + "@github.com/" + process.env.GITHUB_USERNAME + "/" + process.env.GITHUB_REPO_NAME + ".git";

execSync("git -C /home/z/my-project remote set-url origin " + JSON.stringify(url), { stdio: "inherit" });
execSync("git -C /home/z/my-project add -A", { stdio: "inherit" });
execSync('git -C /home/z/my-project commit -m "fix: processStream broken catch block, remove parse-torrent"', { stdio: "inherit" });
execSync("git -C /home/z/my-project push -u origin main --force", { stdio: "inherit" });

console.log("Push complete.");