package Me.Teenaapje.Referral.Commands;

import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import Me.Teenaapje.Referral.Utils.Utils;

public class RefCount extends CommandBase {
	// init class
	public RefCount() {
		 permission = "RefCount";
		 command = "Total";
		 forPlayerOnly = false;
	}
	
	@Override
	public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
		// check arguments
		if (args.length > 2) {
	        Utils.SendMessage(sender, core.config.tooManyArgs);
	        return false;
	    } else if (args.length < 2) {
	    	// check if is player
			if (Utils.IsConsole(sender)) {
		        Utils.SendMessage(sender, core.config.missingPlayer);
				return false;
			}
			
			Player player = (Player)sender;
			String uuid = player.getUniqueId().toString();
			String name = player.getName();
			
			// Show both how many players they've referred and if they've been referred
			int referrals = core.db.GetReferrals(uuid, name);
			int timesReferred = core.db.GetTimesReferred(uuid, name);
			String referredBy = core.db.PlayerReferraldByName(uuid);
			
			Utils.SendMessage(player, core.config.playerTotal);
			
			if (timesReferred > 0) {
				Utils.SendMessage(player, "&6You have been referred by: &f" + referredBy);
			} else {
				Utils.SendMessage(player, "&6You have not been referred by anyone yet.");
			}
	    } else {
	    	// check if the player is online
			Player target = core.GetPlayer(args[1]);
	    	
			if (target == null) {
				Utils.SendMessage(sender, core.config.notOnline);
				return false;
			} else {
				String uuid = target.getUniqueId().toString();
				String name = target.getName();
				
				// Show both how many players they've referred and if they've been referred
				int referrals = core.db.GetReferrals(uuid, name);
				int timesReferred = core.db.GetTimesReferred(uuid, name);
				String referredBy = core.db.PlayerReferraldByName(uuid);
				
				Utils.SendMessage(sender, core.config.playerTotal, target);
				
				if (timesReferred > 0) {
					Utils.SendMessage(sender, "&6" + name + " has been referred by: &f" + referredBy);
				} else {
					Utils.SendMessage(sender, "&6" + name + " has not been referred by anyone yet.");
				}
			}
	    }	
		
		return true;
	}
}