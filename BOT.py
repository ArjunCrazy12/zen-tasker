"""
Discord Task Allocation Bot - MODIFIED VERSION WITH DM FEATURE
============================================================

SETUP INSTRUCTIONS:
1. Discord Bot Setup:
   - Go to https://discord.com/developers/applications
   - Create new application and bot
   - Copy bot token to .env file
   - CRITICAL: Invite bot with BOTH scopes: 'bot' AND 'applications.commands'
   - Permissions needed: Send Messages, Manage Messages, Add Reactions, Manage Roles, Use Slash Commands

2. Google Sheets Setup:
   - Enable Google Sheets API and Google Drive API
   - Create service account, download credentials.json
   - Share sheet with service account email
   - Headers: Task No. | Post link | Comment to post | Assigned user | Proof link

3. Environment Setup:
   Create .env file:
   DISCORD_TOKEN=your_bot_token_here
   TASK_ROLE_NAME=TaskHolder
   REDDIT_CLIENT_ID=your_reddit_client_id
   REDDIT_CLIENT_SECRET=your_reddit_client_secret
   REDDIT_USER_AGENT=your_reddit_user_agent

4. Install: pip install discord.py gspread google-auth python-dotenv asyncpraw flask

5. Usage:
   /configure_settings (Admin only) - Optionally override hardcoded settings.
   /create_task tasks:100 task_type:"Comment" (Admin only) - Start task allocation
"""

import discord, asyncpraw
from discord.ext import commands, tasks
from discord import app_commands
import gspread
from google.oauth2.service_account import Credentials
import os
import re
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict
from dotenv import load_dotenv
from flask import Flask
import threading

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ─── Hardcoded Bot Configuration ──────────────────────────────────────────────
#
# EDIT THESE VALUES FOR YOUR SERVER
#
# --------------------------------------------------------------------------------
VERIFIED_ROLE_NAME = "✶ ⁞𝓥𝓮𝓻𝓲𝓯𝓲𝓮𝓭 ·"  # The name for the role given to users who pass /reddit_verify
VERIFIED_ROLE_ID = 1407582403375136829  # Replace with the Role ID of the role above
DEFAULT_PING_ROLE_NAME = VERIFIED_ROLE_NAME # The default role to ping for new tasks.
DEFAULT_ANNOUNCE_CHANNEL_ID = 1418471239504101487  # Replace with your task announcement channel ID
DEFAULT_LOGS_CHANNEL_ID = 1418601049471848488      # Replace with your bot logs channel ID
DEFAULT_VERIFICATION_CHANNEL_ID = 1418601073471393964 # Replace with your verification logs channel ID
DEFAULT_SHEETS_URL = "https://docs.google.com/spreadsheets/d/1umbEoGpC8by9oAvTmFyS_AH7boTL4C55jla2y6JPCEA/edit?usp=sharing"
BLOCKED_REDDIT_IDS = {"crazy_guy_12"} # A set of lowercase Reddit IDs to block
# --- Bot Behavior Defaults ---
DEFAULT_INTERVAL_MINUTES = 1
DEFAULT_REACTION_TIME_SECONDS = 10
DEFAULT_ROLE_REMOVAL_HOURS = 6
DEFAULT_WINNERS_PER_TASK = 1
MINIMUM_REDDIT_KARMA = 100
# --------------------------------------------------------------------------------

# ─── Keep‑alive HTTP server ──────────────────────────────────────────────────
app = Flask(__name__)

@app.route('/ping')
def ping():
    return '🟢 Bot is alive', 200

def run_webserver():
    port = int(os.environ.get('PORT', 3000))
    app.run(host='0.0.0.0', port=port)
# ────────────────────────────────────────────────────────────────────────────────

# Configuration
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
TASK_ROLE_NAME = os.getenv('TASK_ROLE_NAME', 'TaskHolder')
GOOGLE_SHEETS_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
# Reddit API credentials
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'TaskBot/1.0')

if not DISCORD_TOKEN:
    raise ValueError("DISCORD_TOKEN environment variable is required")

class TaskBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.reactions = True
        intents.guilds = True
        intents.guild_messages = True
        intents.members = True  # Required for Member objects
        
        super().__init__(command_prefix='!', intents=intents)
        
        # Bot state
        self.total_tasks: int = 0
        self.sheet_url: str = DEFAULT_SHEETS_URL
        self.interval_minutes: int = DEFAULT_INTERVAL_MINUTES
        self.configured: bool = False
        
        self.task_allocation_loop: Optional[tasks.Loop] = None
        
        self.announce_channel: Optional[discord.TextChannel] = None
        self.logs_channel: Optional[discord.TextChannel] = None
        self.verification_channel: Optional[discord.TextChannel] = None
        self.current_task: int = 1
        self.gc = None
        self.commands_synced: bool = False
        self.reaction_timestamps: Dict[int, Dict[int, datetime]] = {}
        self.reaction_time: int = DEFAULT_REACTION_TIME_SECONDS
        self.role_removal_hours: int = DEFAULT_ROLE_REMOVAL_HOURS
        self.ping_role_name: str = DEFAULT_PING_ROLE_NAME
        self.winners_per_task: int = DEFAULT_WINNERS_PER_TASK
        self.task_type: str = "Comment"
        self.is_paused: bool = False
        self.task_description: str = ""
        self._role_removal_tasks = set()
        self.stop_requested: bool = False

        
    async def get_member_safely(self, guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
        """Safely get member from guild with fallback to fetch"""
        if not guild:
            return None
            
        member = guild.get_member(user_id)
        if member:
            return member
            
        try:
            logger.info(f"Member {user_id} not in cache for guild {guild.name}. Fetching from API.")
            member = await guild.fetch_member(user_id)
            return member
        except discord.NotFound:
            logger.error(f"User {user_id} not found in guild {guild.name}")
            return None
        except discord.HTTPException as e:
            logger.error(f"Failed to fetch member {user_id}: {e}")
            return None
    
    
    async def setup_hook(self):
        """Initialize connections"""
        logger.info("Running setup_hook...")
        await self.setup_google_sheets()
        
    async def setup_google_sheets(self):
        """Initialize Google Sheets API connection"""
        try:
            if os.path.exists('credentials.json'):
                credentials = Credentials.from_service_account_file(
                    'credentials.json', scopes=GOOGLE_SHEETS_SCOPES
                )
                self.gc = gspread.authorize(credentials)
                logger.info("Google Sheets API initialized successfully.")
            else:
                logger.warning("credentials.json not found. Google Sheets functionality will be disabled.")
        except Exception as e:
            logger.error(f"Google Sheets setup failed: {e}")
            
    async def on_ready(self):
        """Called when bot is ready"""
        logger.info(f'Logged in as {self.user} (ID: {self.user.id})')
        logger.info(f'Bot is in {len(self.guilds)} servers.')

        # Fetch hardcoded channels
        self.announce_channel = self.get_channel(DEFAULT_ANNOUNCE_CHANNEL_ID)
        self.logs_channel = self.get_channel(DEFAULT_LOGS_CHANNEL_ID)
        self.verification_channel = self.get_channel(DEFAULT_VERIFICATION_CHANNEL_ID)
        
        if not self.announce_channel:
            logger.error(f"FATAL: Could not find announce channel with ID {DEFAULT_ANNOUNCE_CHANNEL_ID}. Please check the ID and bot permissions.")
        if not self.logs_channel:
            logger.error(f"FATAL: Could not find logs channel with ID {DEFAULT_LOGS_CHANNEL_ID}. Please check the ID and bot permissions.")
        if not self.verification_channel:
            logger.warning(f"Verification channel with ID {DEFAULT_VERIFICATION_CHANNEL_ID} not found. Verification logging will be disabled.")
        
        # Mark as configured if essential channels are found
        if self.announce_channel and self.logs_channel and self.sheet_url:
            self.configured = True
            logger.info("Bot is configured and ready.")

        if not self.commands_synced:
            await self.sync_commands()
            self.commands_synced = True
        
        await self.cleanup_lingering_roles()

    async def cleanup_lingering_roles(self):
        """Finds and removes the task role from all members on startup."""
        logger.info("Performing startup cleanup of lingering task roles...")
        for guild in self.guilds:
            task_role = discord.utils.get(guild.roles, name=TASK_ROLE_NAME)
            if task_role:
                members_with_role = list(task_role.members) # Create a copy to iterate over
                if not members_with_role:
                    logger.info(f"No members with the '{TASK_ROLE_NAME}' role found in '{guild.name}'.")
                    continue

                logger.warning(f"Found {len(members_with_role)} member(s) with lingering task roles in '{guild.name}'. Removing them...")
                for member in members_with_role:
                    try:
                        await member.remove_roles(task_role, reason="Bot startup cleanup")
                        logger.info(f"Removed role from {member.name}.")
                        await asyncio.sleep(1) # Sleep to avoid rate limits on large servers
                    except discord.Forbidden:
                        logger.error(f"Failed to remove role from {member.name} in '{guild.name}': Missing Permissions.")
                    except discord.HTTPException as e:
                        logger.error(f"Failed to remove role from {member.name} in '{guild.name}': {e}")
        logger.info("Startup role cleanup complete.")

    async def sync_commands(self):
        """Sync slash commands"""
        try:
            logger.info("Attempting to sync slash commands...")
            synced = await self.tree.sync()
            logger.info(f"Synced {len(synced)} commands globally.")
        except Exception as e:
            logger.error(f"Command sync failed: {e}")
            
    def extract_sheet_id(self, url: str) -> Optional[str]:
        """Extract Google Sheets ID from URL"""
        match = re.search(r'/d/([A-Za-z0-9\-_]+)', url)
        return match.group(1) if match else None
        
    async def validate_sheet_access(self, sheet_url: str) -> tuple[bool, str]:
        """Validate Google Sheets access"""
        logger.info(f"Validating Google Sheet access for URL: {sheet_url}")
        if not self.gc:
            logger.warning("Google Sheets API not initialized, validation skipped.")
            return False, "Google Sheets API not initialized"
            
        try:
            sheet_id = self.extract_sheet_id(sheet_url)
            if not sheet_id:
                logger.warning("Invalid Google Sheets URL provided.")
                return False, "Invalid Google Sheets URL"
                
            sheet = self.gc.open_by_key(sheet_id)
            worksheet = sheet.get_worksheet(0)
            worksheet.row_values(1)
            logger.info("Google Sheet access verified successfully.")
            return True, "Sheet access verified"
        except Exception as e:
            logger.error(f"Google Sheet access validation failed: {e}")
            return False, f"Sheet access failed: {e}"
            
    async def get_or_create_role(self, guild: discord.Guild, role_name: str) -> discord.Role:
        """Get or create role"""
        role = discord.utils.get(guild.roles, name=role_name)
        if role:
            logger.info(f"Found existing role '{role_name}' in guild '{guild.name}'.")
        else:
            logger.warning(f"Role '{role_name}' not found in guild '{guild.name}'. Attempting to create it.")
            try:
                role = await guild.create_role(name=role_name, reason="TaskBot role")
                logger.info(f"Successfully created role '{role_name}' in {guild.name}.")
            except discord.Forbidden:
                logger.error(f"Bot lacks permission to create role '{role_name}'.")
                raise
            except Exception as e:
                logger.error(f"Failed to create role '{role_name}': {e}")
                raise
        return role
    
    async def write_to_sheet(self, task_number: int, winner_name: str):
        """Write winner to Google Sheets with retry logic"""
        logger.info(f"Attempting to write to sheet for Task #{task_number} -> Winner: {winner_name}")
        if not self.gc or not self.sheet_url:
            logger.warning("Google Sheets not configured. Skipping write operation.")
            return
            
        for attempt in range(3):
            try:
                sheet_id = self.extract_sheet_id(self.sheet_url)
                if not sheet_id:
                    logger.error("Cannot write to sheet: Invalid sheet URL.")
                    return
                    
                sheet = self.gc.open_by_key(sheet_id)
                worksheet = sheet.get_worksheet(0)
                    
                if task_number + 1 > worksheet.row_count:
                    logger.error(f"Task #{task_number} row is out of bounds for the sheet.")
                    return
                
                worksheet.update_cell(task_number + 1, 4, winner_name)
                logger.info(f"Successfully updated sheet: Task {task_number} assigned to {winner_name}.")
                return
            except Exception as e:
                logger.error(f"Sheet write attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.critical(f"Failed to write to sheet for Task #{task_number} after 3 attempts.")

    async def dm_winner(self, winner_member: discord.Member, task_number: int):
        """DM the winner with sheet link and instructions"""
        logger.info(f"Attempting to DM {winner_member.name} for Task #{task_number}.")
        embed = discord.Embed(
            title="🎉 Congratulations! Task Assigned",
            description=f"You have been assigned **Task #{task_number}**!",
            color=discord.Color.green()
        ).add_field(
            name="📋 Google Sheets Link",
            value=f"[Click here to access the task sheet]({self.sheet_url})",
            inline=False
        ).add_field(
            name="📝 Instructions",
            value="Please fill in the **Proof link** column (Column E) in the sheet once you complete the task.",
            inline=False
        ).add_field(
            name="⏰ Task Details",
            value=f"Task Number: **{task_number}**\nRole: **{TASK_ROLE_NAME}** (will be removed in {self.role_removal_hours} hours)",
            inline=False
        ).set_footer(text="Good luck with your task!")
        
        try:
            await winner_member.send(embed=embed)
            logger.info(f"Successfully sent DM to {winner_member.name}.")
            await self.send_log(f"✅ DM sent to {winner_member.mention} for Task #{task_number}")
            return True
        except (discord.Forbidden, discord.HTTPException) as e:
            logger.warning(f"Cannot DM {winner_member.name}: {e}. Sending info in announce channel.")
            if self.announce_channel:
                await self.announce_channel.send(
                    content=f"{winner_member.mention}, your DMs are closed!",
                    embed=embed
                )
            await self.send_log(f"🚫 DM inaccessible for {winner_member.mention} - sent in channel instead.")
            return False
            
    async def schedule_role_removal(self, member: discord.Member, role: discord.Role):
        """Remove role after configured hours"""
        logger.info(f"Scheduling role '{role.name}' removal for {member.name} in {self.role_removal_hours} hours.")
        await asyncio.sleep(self.role_removal_hours * 3600)
        logger.info(f"Attempting to remove role '{role.name}' from {member.name} now.")
        try:
            # Re-fetch member to handle cases where they might leave and rejoin
            guild = member.guild
            updated_member = await guild.fetch_member(member.id)
            if role in updated_member.roles:
                await updated_member.remove_roles(role, reason=f"TaskBot: Timed {self.role_removal_hours}-hour removal")
                logger.info(f"Removed '{role.name}' from {updated_member.name}.")
                await self.send_log(f"Role '{TASK_ROLE_NAME}' removed from {updated_member.mention} after {self.role_removal_hours} hours.")
            else:
                logger.warning(f"Role '{role.name}' was already removed from {member.name}.")
        except discord.NotFound:
             logger.warning(f"Could not remove role from {member.name} as they have left the server.")
        except Exception as e:
            logger.error(f"Failed to remove role from {member.name}: {e}")
            
    async def restart_task_loop(self):
        """Creates or restarts the task loop with the current interval."""
        logger.info("Creating or restarting task allocation loop...")
        
        if self.task_allocation_loop and self.task_allocation_loop.is_running():
            self.task_allocation_loop.cancel()
            logger.info("Existing task loop cancelled.")

        self.task_allocation_loop = tasks.loop(minutes=self.interval_minutes)(self.task_allocation_loop_impl)
        self.task_allocation_loop.before_loop(self.before_task_loop)
        
        if self.configured and self.announce_channel:
            self.task_allocation_loop.start()
            logger.info(f"Task loop started with a {self.interval_minutes}-minute interval.")
    
    async def on_reaction_add(self, reaction, user):
        """Track reaction timestamps"""
        if user.bot:
            return
            
        message_id = reaction.message.id
        if message_id in self.reaction_timestamps:
            if user.id not in self.reaction_timestamps[message_id]:
                self.reaction_timestamps[message_id][user.id] = datetime.now(timezone.utc)
                logger.info(f"Reaction added by '{user.name}' ({reaction.emoji}) to message {message_id}. Storing timestamp.")
    
    async def task_allocation_loop_impl(self):
        """Implementation of the main task allocation logic"""
        logger.info(f"--- Task Loop Cycle --- Current Task: #{self.current_task}/{self.total_tasks} ---")
        if self.stop_requested:
            logger.info("Loop cycle aborted: stop_requested flag is True.")
            return
        if self.is_paused:
            logger.info("Task allocation is paused. Skipping this cycle.")
            return
        
        if not self.configured or not self.announce_channel:
            logger.warning("Loop is not configured or announce channel not set. Skipping cycle.")
            return
            
        if self.current_task > self.total_tasks:
            logger.info("All tasks have been assigned. Stopping loop.")
            await self.send_log("All tasks have been completed!")

            if self.task_allocation_loop is not None:
                self.task_allocation_loop.stop()
            return
            
        try:
            starting_task_num = self.current_task
            remaining_tasks = self.total_tasks - starting_task_num + 1
            tasks_this_round = min(self.winners_per_task, remaining_tasks)
            
            if tasks_this_round <= 0:
                logger.info("No more tasks to assign. Stopping loop.")
                if self.task_allocation_loop is not None:
                    self.task_allocation_loop.stop()
                return

            logger.info(f"Posting for {tasks_this_round} task(s), starting from #{starting_task_num}.")
            
            # Build announcement embed
            core_description = ""
            people_needed = ""
            if tasks_this_round == 1:
                the_title = "❗・Task Available!"
                core_description = f"React with ✅ within `{self.reaction_time} seconds` to claim task **#{starting_task_num}**!"
                people_needed = "1"
            else:
                the_title = "❗・Tasks Available!"
                task_range = f"**#{starting_task_num}**-**#{starting_task_num + tasks_this_round - 1}**"
                core_description = f"React with ✅ within `{self.reaction_time} seconds` to claim tasks {task_range}!"
                people_needed = str(tasks_this_round)
            
            final_description = f"{self.task_description}\n\n{core_description}" if self.task_description else core_description

            embed = discord.Embed(
                title=the_title,
                description=final_description,
                color=discord.Color.blue()
            ).add_field(name="Task Type", value=self.task_type.title(), inline=True).add_field(name="People Needed", value=people_needed, inline=True)

            message = await self.announce_channel.send(embed=embed)
            self.reaction_timestamps[message.id] = {}
            await message.add_reaction('✅')
            logger.info(f"Task announcement posted in #{self.announce_channel.name} (Message ID: {message.id}). Waiting {self.reaction_time}s for reactions.")
            
            # --- PING LOGIC (Using Role ID for reliability) ---
            role_to_ping = self.announce_channel.guild.get_role(VERIFIED_ROLE_ID)
            if role_to_ping:
                try:
                    await self.announce_channel.send(content=role_to_ping.mention, delete_after=1)
                    logger.info(f"Role ping sent for {role_to_ping.name}")
                except discord.Forbidden:
                    logger.error("Failed to send role ping: Missing 'Manage Messages' permission for delete_after.")
                    await self.send_log("⚠️ **Ping Failed:** The bot needs the **`Manage Messages`** permission in the task channel to send and delete pings.")
                except Exception as ping_error:
                    logger.error(f"Failed to send role ping: {ping_error}")
                    await self.send_log(f"⚠️ An unknown error occurred while sending the role ping.")
            else:
                logger.warning(f"Role with ID '{VERIFIED_ROLE_ID}' not found!")
                await self.send_log(f"⚠️ **Ping Failed:** Could not find the role with ID `{VERIFIED_ROLE_ID}`. Please check the hardcoded configuration.")
            # --- END OF PING LOGIC ---

            await asyncio.sleep(self.reaction_time)
            if self.stop_requested:
                logger.info("Stop command received during reaction period. Halting current task cycle.")
                return
            
            # Process reactions
            logger.info(f"Reaction period ended for message {message.id}. Processing reactions.")
            message = await self.announce_channel.fetch_message(message.id)
            
            reactors = []
            for reaction in message.reactions:
                async for user in reaction.users():
                    if not user.bot:
                        if message.guild is not None:
                            member = await self.get_member_safely(message.guild, user.id)
                            if member and member not in reactors:
                                reactors.append(member)
            logger.info(f"Found {len(reactors)} unique non-bot reactors.")

            task_holder_role = None
            if message.guild is not None:
                task_holder_role = discord.utils.get(message.guild.roles, name=TASK_ROLE_NAME)
            eligible_reactors = [m for m in reactors if not task_holder_role or task_holder_role not in m.roles]
            logger.info(f"{len(eligible_reactors)} eligible reactors after filtering for cooldowns.")

            if eligible_reactors:
                sorted_reactors = sorted(eligible_reactors, key=lambda r: self.reaction_timestamps.get(message.id, {}).get(r.id, datetime.max))
                winners = sorted_reactors[:tasks_this_round]
                logger.info(f"Selected {len(winners)} winner(s): {[w.name for w in winners]}")

                winners_assigned = []
                for i, winner_member in enumerate(winners):
                    # FIXED: Added a try/except block to handle errors for individual winners
                    try:
                        current_task_num = starting_task_num + i
                        logger.info(f"Processing winner {i+1}: {winner_member.name} for Task #{current_task_num}")
                        if message.guild is not None:
                            role = await self.get_or_create_role(message.guild, TASK_ROLE_NAME)
                            await winner_member.add_roles(role, reason=f"TaskBot: Assigned Task #{current_task_num}")
                            
                            removal_task = asyncio.create_task(self.schedule_role_removal(winner_member, role))
                            self._role_removal_tasks.add(removal_task)
                            removal_task.add_done_callback(self._role_removal_tasks.discard)
                            
                            await self.dm_winner(winner_member, current_task_num)
                            await self.write_to_sheet(current_task_num, winner_member.name)
                            winners_assigned.append((winner_member, current_task_num))
                        else:
                            logger.error("Cannot assign role: message.guild is None.")
                    except Exception as e:
                        logger.error(f"Failed to process winner {winner_member.name}: {e}. Skipping to next winner.")

                if winners_assigned:
                    self.current_task += len(winners_assigned)
                    logger.info(f"Assignment complete. New current task is #{self.current_task}.")
                    if len(winners_assigned) == 1:
                        winner, task_num = winners_assigned[0]
                        await self.announce_channel.send(f"> 🥇 Congratulations **{winner.mention}**! You have been assigned Task #{task_num}.")
                    else:
                        mentions = " ".join([w.mention for w, t in winners_assigned])
                        await self.announce_channel.send(f"> 🥇 Congratulations to the winners: {mentions}")
                else:
                    logger.warning("No winners were successfully assigned despite eligible reactors.")
            else:
                logger.info("No eligible reactors found for this task.")
                
                # Create a professional-looking embed for the unclaimed task notification
                unclaimed_tasks_str = f"Task #{starting_task_num}" if tasks_this_round == 1 else f"Tasks #{starting_task_num}-#{starting_task_num + tasks_this_round - 1}"
                minutes_str = f"{self.interval_minutes} minute{'s' if self.interval_minutes > 1 else ''}"

                embed = discord.Embed(
                    title="⚠️ Task Unclaimed",
                    description="No one claimed the available task(s) within the time limit.",
                    color=discord.Color.orange(),
                    timestamp=datetime.now(timezone.utc)
                )
                embed.add_field(name="Details", value=f"**{unclaimed_tasks_str}** received no reactions.", inline=False)
                embed.add_field(name="Next Step", value=f"Reposting in **{minutes_str}**.", inline=False)
                embed.set_footer(text=f"Task Bot | {datetime.now().strftime('%d %B %Y')}")
                
                await self.announce_channel.send(embed=embed)
            
            del self.reaction_timestamps[message.id]
        except Exception as e:
            logger.critical(f"A critical error occurred in the task allocation loop: {e}", exc_info=True)


    async def before_task_loop(self):
        """Wait for bot ready"""
        logger.info("Task loop is waiting for the bot to be ready...")
        await self.wait_until_ready()
        logger.info("Bot is ready. Task loop can now start.")

    async def send_log(self, message: str):
        """Send log message to logs channel"""
        if self.logs_channel:
            try:
                embed = discord.Embed(description=message, color=discord.Color.blue(), timestamp=datetime.now(timezone.utc))
                await self.logs_channel.send(embed=embed)
            except Exception as e:
                logger.error(f"Failed to send log message to channel #{self.logs_channel.name}: {e}")

    async def get_reddit_karma(self, username: str) -> tuple[bool, str, int, int]:
        """Get Reddit user karma"""
        logger.info(f"Fetching Reddit karma for username: {username}")
        if not all([REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET]):
            logger.warning("Reddit API credentials are not configured.")
            return False, "Reddit API not configured", 0, 0
        try:
            async with asyncpraw.Reddit(client_id=REDDIT_CLIENT_ID, client_secret=REDDIT_CLIENT_SECRET, user_agent=REDDIT_USER_AGENT) as reddit:
                user = await reddit.redditor(username)
                await user.load()
                logger.info(f"Successfully fetched karma for u/{username}: Post {user.link_karma}, Comment {user.comment_karma}")
                return True, "Success", user.link_karma, user.comment_karma
        except Exception as e:
            logger.error(f"Error fetching karma for u/{username}: {e}")
            return False, f"Error: {e}", 0, 0
    
    def check_admin_permissions(self, member: discord.Member) -> bool:
        """Check if member has admin permissions"""
        return member.guild_permissions.administrator
        
bot = TaskBot()

def admin_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild: return False
        member = await bot.get_member_safely(interaction.guild, interaction.user.id)
        if not member: return False
        is_admin = bot.check_admin_permissions(member)
        if not is_admin:
            logger.warning(f"User '{member.name}' (ID: {member.id}) tried to use an admin command without permissions.")
        return is_admin
    return app_commands.check(predicate)

@bot.tree.command(name="configure_settings", description="Optionally override the bot's hardcoded settings (Admin only)")
@app_commands.describe(
    interval_minutes=f"Override the time between tasks in minutes (Default: {DEFAULT_INTERVAL_MINUTES}).",
    role_removal_hours=f"Override hours before the TaskHolder role is removed (Default: {DEFAULT_ROLE_REMOVAL_HOURS}).",
    reaction_time=f"Override time users have to react in seconds (Default: {DEFAULT_REACTION_TIME_SECONDS}).",
    announce_channel="Override the default task announcement channel (Set by ID in the code).",
    logs_channel="Override the default bot logs channel (Set by ID in the code).",
    sheets_url="Override the default Google Sheets URL (Set in the code).",
    ping_role_name=f"Override the default role to ping for new tasks (Default: \"{DEFAULT_PING_ROLE_NAME}\")."
)
@admin_only()
async def configure_settings(
    interaction: discord.Interaction,
    interval_minutes: Optional[int] = None,
    role_removal_hours: Optional[int] = None,
    reaction_time: Optional[int] = None,
    announce_channel: Optional[discord.TextChannel] = None,
    logs_channel: Optional[discord.TextChannel] = None,
    sheets_url: Optional[str] = None,
    ping_role_name: Optional[str] = None
):
    guild_name = interaction.guild.name if interaction.guild else "Direct Message"
    logger.info(f"'/configure_settings' invoked by '{interaction.user.name}' in guild '{guild_name}'.")
    await interaction.response.defer(ephemeral=True)
    
    # --- Validation for optional inputs ---
    if interval_minutes is not None and interval_minutes < 1:
        await interaction.followup.send("❌ Interval must be at least 1 minute.", ephemeral=True)
        return
    if reaction_time is not None and not (1 <= reaction_time <= 60):
        await interaction.followup.send("❌ Reaction time must be between 1-60 seconds.", ephemeral=True)
        return
    if role_removal_hours is not None and not (1 <= role_removal_hours <= 168):
        await interaction.followup.send("❌ Role removal hours must be between 1-168 hours (1 week).", ephemeral=True)
        return

    # --- Update settings only if a value was provided ---
    if interval_minutes is not None:
        bot.interval_minutes = interval_minutes
        logger.info(f"Interval overridden to: {interval_minutes} minutes")
    if role_removal_hours is not None:
        bot.role_removal_hours = role_removal_hours
        logger.info(f"Role removal time overridden to: {role_removal_hours} hours")
    if reaction_time is not None:
        bot.reaction_time = reaction_time
        logger.info(f"Reaction time overridden to: {reaction_time} seconds")
    if announce_channel:
        bot.announce_channel = announce_channel
        logger.info(f"Announce channel overridden to: #{announce_channel.name}")
    if logs_channel:
        bot.logs_channel = logs_channel
        logger.info(f"Logs channel overridden to: #{logs_channel.name}")
    if sheets_url:
        bot.sheet_url = sheets_url
        logger.info(f"Sheets URL overridden to: {sheets_url}")
    if ping_role_name:
        bot.ping_role_name = ping_role_name
        logger.info(f"Ping role name overridden to: {ping_role_name}")

    bot.configured = True
    
    # --- Restart the loop if the interval was changed and the loop is active ---
    if interval_minutes is not None and bot.task_allocation_loop and bot.task_allocation_loop.is_running():
        logger.info("Interval was changed, restarting the task loop to apply it.")
        await bot.restart_task_loop()
    
    await interaction.followup.send("✅ Bot settings updated successfully! Any unset options will use the hardcoded defaults.", ephemeral=True)

@bot.tree.command(name="create_task", description="Create and start task allocation (Admin only)")
@app_commands.describe(
    tasks="Total number of tasks",
    task_type="Type of task",
    winners_per_task="Number of winners per task round (defaults to the hardcoded value).",
    description="An optional description to send with each task announcement."
)
@admin_only()
async def create_task(
    interaction: discord.Interaction,
    tasks: int,
    task_type: str,
    winners_per_task: Optional[int] = None,
    description: Optional[str] = None
):
    guild_name = interaction.guild.name if interaction.guild else "Direct Message"
    logger.info(f"'/create_task' invoked by '{interaction.user.name}' in guild '{guild_name}' with params: tasks={tasks}, type='{task_type}', winners={winners_per_task}.")
    await interaction.response.defer()
    
    if not bot.configured:
        logger.warning("'/create_task' failed: Bot is not configured. Please check hardcoded channel IDs.")
        await interaction.followup.send("❌ **Bot not configured!** Please ensure the hardcoded channel IDs in the code are correct.", ephemeral=True)
        return
        
    bot.total_tasks = tasks
    bot.current_task = 1
    bot.winners_per_task = winners_per_task if winners_per_task is not None else DEFAULT_WINNERS_PER_TASK
    bot.task_type = task_type
    bot.is_paused = False
    bot.stop_requested = False
    bot.task_description = description if description else ""
    
    await interaction.followup.send(f"✅ Task allocation started for {tasks} tasks!")
    await bot.send_log(f"Task allocation started by {interaction.user.mention} for {tasks} tasks.")
    await bot.restart_task_loop()

@bot.tree.command(name="test_bot", description="Test if bot is working")
async def test_bot(interaction: discord.Interaction):
    logger.info(f"'/test_bot' invoked by '{interaction.user.name}'.")
    await interaction.response.send_message("✅ Bot is online and responsive!", ephemeral=True)

@bot.tree.command(name="stop_tasks", description="Stop current task execution (Admin only)")
@admin_only()
async def stop_tasks(interaction: discord.Interaction):
    logger.info(f"'/stop_tasks' invoked by '{interaction.user.name}'.")
    await interaction.response.defer(ephemeral=True)
    bot.stop_requested = True
    if bot.task_allocation_loop and bot.task_allocation_loop.is_running():
        bot.task_allocation_loop.stop()
        logger.info("Task allocation stopped by admin command.")
        await bot.send_log(f"🛑 Task allocation stopped by {interaction.user.mention}.")
        await interaction.followup.send("✅ Task allocation has been stopped.", ephemeral=True)
    else:
        logger.warning("'/stop_tasks' used but loop was not running.")
        await interaction.followup.send("❌ Task allocation is not currently running.", ephemeral=True)

@bot.tree.command(name="pause_tasks", description="Temporarily pause task allocation (Admin only)")
@admin_only()
async def pause_tasks(interaction: discord.Interaction):
    logger.info(f"'/pause_tasks' invoked by '{interaction.user.name}'.")
    await interaction.response.defer(ephemeral=True)
    if not bot.is_paused:
        bot.is_paused = True
        logger.info("Task allocation paused.")
        await bot.send_log(f"⏸️ Task allocation paused by {interaction.user.mention}.")
        await interaction.followup.send("✅ Task allocation has been paused.", ephemeral=True)
    else:
        await interaction.followup.send("⚠️ Task allocation is already paused.", ephemeral=True)

@bot.tree.command(name="resume_tasks", description="Resume task allocation (Admin only)")
@admin_only()
async def resume_tasks(interaction: discord.Interaction):
    logger.info(f"'/resume_tasks' invoked by '{interaction.user.name}'.")
    await interaction.response.defer(ephemeral=True)
    if bot.is_paused:
        bot.is_paused = False
        logger.info("Task allocation resumed.")
        await bot.send_log(f"▶️ Task allocation resumed by {interaction.user.mention}.")
        await interaction.followup.send("✅ Task allocation has been resumed.", ephemeral=True)
    else:
        await interaction.followup.send("⚠️ Task allocation is not currently paused.", ephemeral=True)
        
@bot.tree.command(name="bot_info", description="Show bot information")
async def bot_info(interaction: discord.Interaction):
    logger.info(f"'/bot_info' invoked by '{interaction.user.name}'.")
    embed = discord.Embed(title="Bot Information", color=discord.Color.blue())
    embed.add_field(name="Current Task", value=str(bot.current_task), inline=True)
    embed.add_field(name="Total Tasks", value=str(bot.total_tasks), inline=True)
    embed.add_field(name="Interval", value=f"{bot.interval_minutes} minutes", inline=True)
    embed.add_field(name="Reaction Time", value=f"{bot.reaction_time} seconds", inline=True)
    embed.add_field(name="Role Removal", value=f"{bot.role_removal_hours} hours", inline=True)
    embed.add_field(name="Announce Channel", value=bot.announce_channel.mention if bot.announce_channel else "Not set", inline=True)
    embed.add_field(name="Logs Channel", value=bot.logs_channel.mention if bot.logs_channel else "Not set", inline=True)
    embed.add_field(name="Sheet Connected", value="✅" if bot.gc else "❌", inline=True)
    embed.add_field(name="Loop Running", value="✅" if bot.task_allocation_loop and bot.task_allocation_loop.is_running() else "❌", inline=True)
    embed.add_field(name="Winners Per Task", value=str(bot.winners_per_task), inline=True)
    embed.add_field(name="Task Type", value=bot.task_type.title(), inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="reddit_verify", description="Verify Reddit account and get verified role")
@app_commands.describe(reddit_id="Reddit username (without u/)")
async def reddit_verify(interaction: discord.Interaction, reddit_id: str):
    logger.info(f"'/reddit_verify' invoked by '{interaction.user.name}' for Reddit user '{reddit_id}'.")
    await interaction.response.defer(ephemeral=True)
    
    if not interaction.guild:
        await interaction.followup.send("❌ This command can only be used in a server.", ephemeral=True)
        return
        
    clean_username = reddit_id.replace("u/", "").replace("/u/", "").strip()
    
    if clean_username.lower() in BLOCKED_REDDIT_IDS:
        logger.warning(f"User '{interaction.user.name}' attempted to verify a blocked Reddit ID: '{clean_username}'.")
        await interaction.followup.send("❌ This Reddit account is not eligible for verification.", ephemeral=True)
        return
    
    if not clean_username:
        await interaction.followup.send("❌ Please provide a valid Reddit username.", ephemeral=True)
        return
        
    member = await bot.get_member_safely(interaction.guild, interaction.user.id)
    if not member:
        await interaction.followup.send("❌ Could not find you in this server.", ephemeral=True)
        return
        
    success, message, link_karma, comment_karma = await bot.get_reddit_karma(clean_username)
    
    if success:
        total_karma = link_karma + comment_karma
        if total_karma >= MINIMUM_REDDIT_KARMA:
            try:
                verified_role = await bot.get_or_create_role(interaction.guild, VERIFIED_ROLE_NAME)
                await member.add_roles(verified_role, reason=f"Reddit verification: u/{clean_username}")
                await interaction.followup.send("✅ You have been successfully verified!", ephemeral=True)

                # --- START: MODIFIED LOGGING BLOCK ---
                if bot.verification_channel:
                    try:  
                        verification_message = (
                            f"**Discord:** {member.mention} (`{member.name}`) \n"
                            f"**Reddit:** [u/{clean_username}](https://www.reddit.com/user/{clean_username}/) \n"
                            f"**Total Karma:** {total_karma:,}\n"
                        )
                        await bot.verification_channel.send(verification_message)
                        logger.info(f"Sent verification log to channel #{bot.verification_channel.name}")
                    except Exception as log_error:
                        # FIXED: This now sends a detailed error to your main logs channel if it fails.
                        error_report = (
                            f"⚠️ **Could not send log to Verification Channel (`#{bot.verification_channel.name}`)**\n\n"
                            f"**Error:** ```{log_error}```\n"
                            "**Most Likely Cause:** The bot is missing the **`Embed Links`** permission in that channel."
                        )
                        logger.error(f"Failed to send verification log: {log_error}")
                        await bot.send_log(error_report)
                else:
                    logger.warning("Verification channel not configured or found, skipping verification log.")
                # --- END: MODIFIED LOGGING BLOCK ---
                
                await bot.send_log(f"✅ Reddit verification successful: {member.mention} verified as u/{clean_username} with {total_karma:,} karma.")

            except Exception as e:
                logger.error(f"Error during verification process for {member.name}: {e}")
                await interaction.followup.send("❌ An unexpected error occurred during the final verification step.", ephemeral=True)
        else:
            await interaction.followup.send(f"❌ Your Reddit account does not meet the minimum requirement of {MINIMUM_REDDIT_KARMA} karma. Your total karma is {total_karma}.", ephemeral=True)
    else:
        await interaction.followup.send(f"❌ Could not verify Reddit account: {message}", ephemeral=True)

@bot.tree.command(name="manual_assign", description="Manually assign a task to a user (Admin only)")
@app_commands.describe(task_number="The task number to assign", user="The user to assign the task to")
@admin_only()
async def manual_assign(interaction: discord.Interaction, task_number: int, user: discord.Member):
    logger.info(f"'/manual_assign' invoked by '{interaction.user.name}' for Task #{task_number} to user '{user.name}'.")
    await interaction.response.defer(ephemeral=True)

    if not bot.configured:
        await interaction.followup.send("❌ The bot must be configured first.", ephemeral=True)
        return

    # FIXED: Check if tasks have been created before allowing manual assignment
    if bot.total_tasks == 0:
        await interaction.followup.send("❌ You must start a task batch with `/create_task` before you can manually assign one.", ephemeral=True)
        return
        
    if not (1 <= task_number <= bot.total_tasks):
        await interaction.followup.send(f"❌ Invalid task number. Please provide a number between 1 and {bot.total_tasks}.", ephemeral=True)
        return
        
    if not interaction.guild:
        return

    try:
        role = await bot.get_or_create_role(interaction.guild, TASK_ROLE_NAME)
        await user.add_roles(role, reason=f"TaskBot: Manual assignment by {interaction.user.name}")
        
        removal_task = asyncio.create_task(bot.schedule_role_removal(user, role))
        bot._role_removal_tasks.add(removal_task)
        removal_task.add_done_callback(bot._role_removal_tasks.discard)

        await bot.dm_winner(user, task_number)
        await bot.write_to_sheet(task_number, user.name)
        
        await bot.send_log(f"✍️ Task #{task_number} was manually assigned to {user.mention} by {interaction.user.mention}.")
        await interaction.followup.send(f"✅ Successfully assigned Task #{task_number} to {user.mention}.", ephemeral=True)

    except discord.Forbidden:
        logger.error("Bot lacks permissions for manual assignment.")
        await interaction.followup.send("❌ **Permission Error!** The bot needs 'Manage Roles' and its role must be higher than 'TaskHolder'.", ephemeral=True)
    except Exception as e:
        logger.error(f"Failed to manually assign task: {e}")
        await interaction.followup.send(f"❌ An unexpected error occurred: {e}", ephemeral=True)

@configure_settings.error
@create_task.error
@stop_tasks.error
@pause_tasks.error
@resume_tasks.error
@manual_assign.error
async def admin_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        await interaction.response.send_message("❌ You do not have administrator permissions to use this command.", ephemeral=True)
    else:
        logger.error(f"An admin command failed: {error}", exc_info=True)
        if not interaction.response.is_done():
            await interaction.response.send_message("❌ An unexpected error occurred.", ephemeral=True)
        else:
            await interaction.followup.send("❌ An unexpected error occurred.", ephemeral=True)
        
if __name__ == "__main__":
    try:
        web_thread = threading.Thread(target=run_webserver, daemon=True)
        web_thread.start()
        bot.run(DISCORD_TOKEN)
    except Exception as e:
        logger.critical(f"Bot failed to start: {e}", exc_info=True)
