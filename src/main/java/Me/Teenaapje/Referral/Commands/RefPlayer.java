package Me.Teenaapje.Referral.Commands;

import java.util.UUID;

import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import Me.Teenaapje.Referral.Utils.ConfigManager;
import Me.Teenaapje.Referral.Utils.Utils;

public class RefPlayer extends CommandBase {
	// init class
	public RefPlayer() {
		permission = "ReferPlayer";
		command = "";
		forPlayerOnly = true;
	}

	@Override
	public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
		Player player = (Player)sender;

		// check arguments
		if (args.length > 1) {
			Utils.SendMessage(player, core.config.tooManyArgs);
			return false;
		} else if (args.length < 1) {
			Utils.SendMessage(player, core.config.missingPlayer);
			return false;
		}

		// Check if player already referred a player
		if (core.db.PlayerReferrald(player.getUniqueId().toString(), player.getName())) {
			Utils.SendMessage(player, core.config.alreadyRefedSelf);
			return false;
		}

		// Get the target player's UUID
		UUID targetUUID = core.GetPlayerUUID(args[0]);
		if (targetUUID == null) {
			Utils.SendMessage(player, core.config.playerNeverPlayed.replace("%player_name%", args[0]));
			return false;
		}

		// Get the target player (might be null if offline)
		Player target = core.GetPlayer(args[0]);

		// Check if the player wants to referral themselves
		if (player.getUniqueId().equals(targetUUID)) {
			Utils.SendMessage(player, core.config.referSelf);
			return false;
		}

		// Check if player is referred
		if (!ConfigManager.canReferEachOther && core.db.PlayerReferrald(targetUUID.toString(), args[0])) {
			String refedUUID = core.db.PlayerReferraldBy(targetUUID.toString());
			// Check if the player try to refer each other
			if (refedUUID != null && refedUUID.equalsIgnoreCase(player.getUniqueId().toString())) {
				Utils.SendMessage(player, core.config.refEachOther);
				return false;
			}
		}

		float playTime = (player.getLastPlayed() - player.getFirstPlayed()) / 60000;

		// Check if server uses time limit if so is player in time?
		if (ConfigManager.useReferralTimeLimit && playTime > ConfigManager.referralTimeLimit) {
			Utils.SendMessage(player, core.config.referTimeOut);
			return false;
		}

		// Check if server uses time limit if so did the player play enough
		if (ConfigManager.useReferralMinPlay && playTime < ConfigManager.referralMinPlay) {
			Utils.SendMessage(player, core.config.referMinPlay);
			return false;
		}

		// Check if server uses max same ip - only possible if target is online
		if (target != null && ConfigManager.useSameIPLimit) {
			String hostName = player.getAddress().getHostName();

			// Check if server uses time limit if so is player in time?
			if (ConfigManager.maxSameIP == 0) {
				// check if users have the same ip
				if (hostName.compareTo(target.getAddress().getHostName()) == 0) {
					// cant use the same network
					Utils.SendMessage(player, core.config.maxIP);
					return false;
				}
			} else if (ConfigManager.maxSameIP <= core.db.GetUsedRefIP(player.getUniqueId().toString(), hostName)) {
				// cant use the same network
				Utils.SendMessage(player, core.config.maxIP);
				return false;
			}
		}

		// Process the referral directly without confirmation
		if (target == null) {
			// Store the referral in the database even if target is offline
			try {
				// Get the target name from UUID if possible
				String targetName = core.GetPlayerName(targetUUID);
				if (targetName == null) targetName = args[0];

				// Store referrer's UUID and referred player's UUID
				core.db.StoreOfflineReferral(player.getUniqueId().toString(), player.getName(), targetUUID.toString(), targetName);

				// Give rewards to the referring player
				core.UseCommands(ConfigManager.playerReferd, player);

				// Notify the player
				Utils.SendMessage(player, core.config.offlineReferralSent.replace("%player_name%", targetName));

				// Check for milestone rewards for the referring player
				if (ConfigManager.useMileStoneRewards) {
					String playerUUID = player.getUniqueId().toString();
					String playerName = player.getName();

					int playerLastReward = core.db.GetLastReward(playerUUID, playerName);
					int playerReferrals = core.db.GetReferrals(playerUUID, playerName) + 1; // Add 1 to account for the new referral

					// Update the last reward count in the database
					core.db.LastRewardUpdate(player, playerReferrals);

					// Check if player has a new milestone reward
					if (core.milestone.HasAReward(playerLastReward, playerReferrals)) {
						core.UseCommands(core.milestone.GetRewards(playerReferrals), player);
					}
				}

				// Log the action for debugging
				Utils.Console("Player " + player.getName() + " referred offline player " + targetName);

				return true;
			} catch (Exception e) {
				e.printStackTrace();
				Utils.SendMessage(player, core.config.referralError);
				return false;
			}
		} else {
			// Target is online, process normally
			try {
				// When player refers target, target's UUID should be stored in player's record
				core.db.ReferralPlayer(player, target);

				Utils.SendMessage(target, core.config.referring);
				Utils.SendMessage(player, core.config.successfulOnlineReferral.replace("%player_name%", target.getName()));

				// Give rewards to both players
				core.UseCommands(ConfigManager.playerRefers, target);
				core.UseCommands(ConfigManager.playerReferd, player);

				// Check for milestone rewards
				if (ConfigManager.useMileStoneRewards) {
					// Check for target player
					String targetPlayerUUID = target.getUniqueId().toString();
					String targetPlayerName = target.getName();

					int targetLastReward = core.db.GetLastReward(targetPlayerUUID, targetPlayerName);
					int targetReferrals = core.db.GetReferrals(targetPlayerUUID, targetPlayerName);

					// Update the last reward count in the database for target
					if (core.milestone.HasAReward(targetLastReward, targetReferrals)) {
						core.db.LastRewardUpdate(target, targetReferrals);
						core.UseCommands(core.milestone.GetRewards(targetReferrals), target);
					}

					// Check for referring player
					String playerUUID = player.getUniqueId().toString();
					String playerName = player.getName();

					int playerLastReward = core.db.GetLastReward(playerUUID, playerName);
					int playerReferrals = core.db.GetReferrals(playerUUID, playerName);

					// Update the last reward count in the database for referrer
					if (core.milestone.HasAReward(playerLastReward, playerReferrals)) {
						core.db.LastRewardUpdate(player, playerReferrals);
						core.UseCommands(core.milestone.GetRewards(playerReferrals), player);
					}
				}

				// Log the action for debugging
				Utils.Console("Player " + player.getName() + " referred online player " + target.getName());

				return true;
			} catch (Exception e) {
				e.printStackTrace();
				Utils.SendMessage(player, core.config.referralError);
				return false;
			}
		}
	}
}