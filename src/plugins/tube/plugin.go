/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 1/15/13
 * Time: 11:34 PM
 * To change this template use File | Settings | File Templates.
 */
package tube
//
//import (
//	"db"
//	gob "encoding/gob"
//)
//
//
//
//// Serializer for the data struct
//func (s *Tube)Serialize(g *gob.Encoder) error {
//
//	return nil
//}
//
//const T_Tube = "TUBE"
//
////The plugin itself. If it has no state, it's just an empty struct
//type TubePlugin struct {
//
//}
//
//
////Object creator. Whenever we receive a writing command for a type, we ask the responsible plugin to create an object of that type.
////We can make decisions on what to do here based on the command name given to use.
////We must return the type of the object and the actual entry
//func (p *TubePlugin)CreateObject(commandName string) (*db.Entry, string) {
//
//	ret := &db.Entry{ Value: &Tube{} }
//
//	return ret, T_Tube
//}
//
//func HandleMKTUBE(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {
//
//	// we must cast the entry value to the struct we're suppposed to
//	_ = entry.Value.(*Tube)
//
//	return db.NewResult(db.NewStatus("OK"))
//
//}
//
//func HandleTUBEREAD(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {
//
//	return nil
//}
//
//func HandleTUBEWRITE(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {
//
//	return nil
//}
//
//
//
////Deserialize and create a db entry.
////This is used for loading objects from a dump, and must be implemented at least as a stub.
////We use a gob decoder to decode the buffer
//func (p *TubePlugin)LoadObject(buf []byte, typeName string) *db.Entry {
//
//	return nil
//
//}
//
////
////Get the plugin manifest for the plugin
////A manifest declares the plugin, commmands and types it is responsible for
//func (p *TubePlugin)GetManifest() db.PluginManifest {
//
//	return db.PluginManifest {
//
//		Name: "Tube",
//		Types: []string{ T_Tube, },
//		Commands:  []db.CommandDescriptor {
//			db.CommandDescriptor{
//				CommandName: "FOO",
//				MinArgs: 0,	MaxArgs: 0,
//				Handler: HandleFOO,
//				CommandType: db.CMD_WRITER,
//			},
//		},
//	}
//
//}
//
////String representation of the plugin to support %s formatting
//func (p* TubePlugin) String() string {
//	return T_Tube
//}
